/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import com.yammer.metrics.core.Timer
import kafka.common.LogSegmentOffsetOverflowException
import kafka.utils._
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{BufferSupplier, Time, Utils}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, CompletedTxn, FetchDataInfo, LazyIndex, LogConfig, LogOffsetMetadata, OffsetIndex, OffsetPosition, ProducerStateManager, RollParams, TimeIndex, TimestampOffset, TransactionIndex, TxnIndexSearchResult}

import java.io.{File, IOException}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, NoSuchFileException}
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The file records containing log entries
 * @param lazyOffsetIndex The offset index
 * @param lazyTimeIndex The timestamp index
 * @param txnIndex The transaction index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val lazyOffsetIndex: LazyIndex[OffsetIndex],
                               val lazyTimeIndex: LazyIndex[TimeIndex],
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int,
                               val rollJitterMs: Long,
                               val time: Time) extends Logging {

  def offsetIndex: OffsetIndex = lazyOffsetIndex.get

  def timeIndex: TimeIndex = lazyTimeIndex.get

  def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (lazyOffsetIndex.file.exists) {
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      // Sanity checks for time index and offset index are skipped because
      // we will recover the segments above the recovery point in recoverLog()
      // in any case so sanity checking them here is redundant.
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${lazyOffsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  private var bytesSinceLastIndexEntry = 0

  // The timestamp we used for time based log rolling and for ensuring max compaction delay
  // volatile for LogCleaner to see the update
  @volatile private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp and offset we see so far */
  @volatile private var _maxTimestampAndOffsetSoFar: TimestampOffset = TimestampOffset.UNKNOWN
  def maxTimestampAndOffsetSoFar_= (timestampOffset: TimestampOffset): Unit = _maxTimestampAndOffsetSoFar = timestampOffset
  def maxTimestampAndOffsetSoFar: TimestampOffset = {
    if (_maxTimestampAndOffsetSoFar == TimestampOffset.UNKNOWN)
      _maxTimestampAndOffsetSoFar = timeIndex.lastEntry
    _maxTimestampAndOffsetSoFar
  }

  /* The maximum timestamp we see so far */
  def maxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.timestamp
  }

  def offsetOfMaxTimestampSoFar: Long = {
    maxTimestampAndOffsetSoFar.offset
  }

  /* Return the size in bytes of this log segment */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param largestOffset The last offset in the message set
   *                      待写入的一个批次的消息的最大位移值
   * @param largestTimestamp The largest timestamp in the message set.
   *                         待写入的一个批次的消息的最大时间戳
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   *                                    待写入的一个批次的消息的最大时间戳对应消息的位移
   * @param records The log entries to append.
   *                待写入的消息集合
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   */
  @nonthreadsafe
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    //1、判断待写入的消息集合是否为空，不为空进行下面操作
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")

      // 判断当前日志段是否为空
      // 如果是空，则记录要写入消息集合的最大时间戳，并将其作为后面新增日志段倒计时的依据
      val physicalPosition = log.sizeInBytes()
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)

      //2、判断输入参数最大位移值是否合法,标准是largestOffset-baseOffset是否在整数范围内
      ensureOffsetInRange(largestOffset)

      // append the messages
      //3、如果合法，追加消息(执行真正的写入)，将内存中的消息对象写入到os的页缓存
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      //4、更新内存中的最大时间戳和相应的偏移量
      //每个日志段都要保存当前最大时间戳和所属消息的偏移信息
      //Broker 端提供有定期删除日志的功能。比如我只想保留最近 7 天日志，就是基于当前最大时间戳值。
      //而最大时间戳对应的消息的偏移值则用于时间戳索引项。时间戳索引项保存时间戳与消息偏移的对应关系。该步骤中，Kafka更新并保存这组对应关系。
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampAndOffsetSoFar = new TimestampOffset(largestTimestamp, shallowOffsetOfMaxTimestamp)
      }
      // append an entry to the index (if needed)
      //5、判断是否需要新增索引项，标准：已写入字节数是否超过4KB
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        //调用索引对象的append方法新增偏移、时间戳索引项
        // 当已写入字节数超过了 4KB 之后，append方法会调用索引对象的append方法新增索引项，同时清空已写入字节数，以备下次重新累积计算
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        //已写字节数置零，用于下次重新计算
        bytesSinceLastIndexEntry = 0
      }
      // 若不需要新增索引项，则更新日志段已写入的字节数
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    if (!canConvertToRelativeOffset(offset))
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION)
      val maybeCompletedTxn = appendInfo.append(batch, Optional.empty())
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.ifPresent(completedTxn => {
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
        producerStateManager.completeTxn(completedTxn)
      })
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    val mapping = offsetIndex.lookup(offset)
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   * 从第一个 偏移量>=startOffset 开始的该段中读取消息集。消息集将包括不超过maxSize字节，并且如果指定了maxOffset，则将在maxOffset之前结束。
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   *                    要读取的第一条消息的偏移
   * @param maxSize The maximum number of bytes to include in the message set we read
   *                能读取的最大字节数
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   *                    日志段中能读到的最大文件位置
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *                      是否允许在消息体过大时至少返回第一条消息，该参数为true：即时出现消息体字节数超过maxSize，read方法依然能返回至少一条消息，主要是为了确保不出现消费饿死情况
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   *         偏移量 >= startOffset 的第一条消息的获取的数据和偏移量元数据，如果startOffset大于此日志中的最大偏移量，则为null
   */
  @threadsafe
  def read(startOffset: Long,
           maxSize: Int,
           maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    // 判断待读取的最大字节数是否合法
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    //1、输入参数 startOffset 仅仅是偏移量，kafka需根据索引信息找到对应的物理文件位置才能开始读取消息
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    // 如果起始位置已在日志末尾，则返回null
    if (startOffsetAndSize == null)
      return null

    //2、定位要读取的起始文件位置
    val startPosition = startOffsetAndSize.position
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    // 计算调整后的能读取的最大字节数，如果该值为空，则返回空
    if (adjustedMaxSize == 0)
      return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 根据是否给maxOffset来计算要读取的消息集长度
    /**
     * 待确定了读取起始位置，日志段代码需要根据这部分信息以及 maxSize 和 maxPosition 参数共同计算要读取的总字节数。
     * 举个例子，假设 maxSize=100，maxPosition=300，startPosition=250，那么 read 方法只能读取 50 字节，
     * 因为 maxPosition - startPosition = 50。我们把它和maxSize参数相比较，其中的最小值就是最终能够读取的总字节数。
     */
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    //3、调用FileRecords的slice方法，从指定位置读取指定大小的消息集合
    new FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      adjustedMaxSize < startOffsetAndSize.size, Optional.empty())
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Optional[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
   * from the end of the log and index.
   *
   * 什么是恢复日志段？
   * Broker 在启动时会从磁盘上加载所有日志段信息到内存中，并创建相应的 LogSegment 对象实例。是Broker重启后恢复日志段的操作逻辑。
   *
   * 注意该操作在执行过程中要读取日志段文件。因此，若你的环境有很多日志段文件，你又发现Broker重启很慢，那你现在就知道了，这是因为Kafka在执行recover的过程中需要读取大量磁盘文件。
   *
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index.
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    //1、依次调用索引对象的reset方法，清空所有的索引文件
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN
    try {
      //2、遍历日志段中的所有消息集或消息批次(RecordBatch)
      for (batch <- log.batches.asScala) {
        //2.1、校验消息集，该集合中的消息必须要符合kafka定义的二进制格式
        batch.ensureValid()
        //该set中最后一条消息的偏移值不能越界，即它与日志段起始偏移的差值必须是一个正整数
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        //最大时间戳在批处理级别暴漏，因此无需迭代记录
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampAndOffsetSoFar = new TimestampOffset(batch.maxTimestamp, batch.lastOffset)
        }

        // Build offset index
        // 简历偏移索引
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          //更新索引项
          lastIndexEntry = validBytes
        }
        //不断累加更新当前已读取的消息字节数
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch >= 0 && cache.latestEpoch.asScala.forall(batch.partitionLeaderEpoch > _)) {
              // 更新Leader Epoch缓存
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
            }
          }
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e@ (_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s. %s"
          .format(log.file.getAbsolutePath, validBytes, e.getMessage, e.getCause))
    }
    /**
     * 3、遍历执行完成后
     * 将日志段当前总字节数 sizeInBytes 和刚刚累加的已读取字节数 validBytes 进行比较
     * 若前者>后者，则说明日志段写入了一些非法消息，需截断，将日志段大小调为合法数值
     */
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    log.truncateTo(validBytes)
    //截断处理后，还必须相应地调整索引文件大小
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampAndOffsetSoFar = lastTimeIndexEntry

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampAndOffsetSoFar = new TimestampOffset(maxTimestampOffsetAfterLastEntry.timestamp, maxTimestampOffsetAfterLastEntry.offset)
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "LogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestRecordTimestamp=" + largestRecordTimestamp +
    ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    val mapping = translateOffset(offset)
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush(): Unit = {
    LogFlushStats.logFlushTimer.time { () =>
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateParentDir(dir: File): Unit = {
    log.updateParentDir(dir)
    lazyOffsetIndex.updateParentDir(dir)
    lazyTimeIndex.updateParentDir(dir)
    txnIndex.updateParentDir(dir)
  }

  /**
   * Change the suffix for the index and log files for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    log.renameTo(new File(Utils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    lazyOffsetIndex.renameTo(new File(Utils.replaceSuffix(lazyOffsetIndex.file.getPath, oldSuffix, newSuffix)))
    lazyTimeIndex.renameTo(new File(Utils.replaceSuffix(lazyTimeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  def hasSuffix(suffix: String): Boolean = {
    log.file.getName.endsWith(suffix) &&
    lazyOffsetIndex.file.getName.endsWith(suffix) &&
    lazyTimeIndex.file.getName.endsWith(suffix) &&
    txnIndex.file.getName.endsWith(suffix)
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
    * If not previously loaded,
    * load the timestamp of the first message into memory.
    */
  private def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long): Long = {
    // Load the timestamp of the first message into memory
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
    * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
    */
  def getFirstBatchTimestamp(): Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset))
  }

  /**
   * Close this log segment
   */
  def close(): Unit = {
    if (_maxTimestampAndOffsetSoFar != TimestampOffset.UNKNOWN)
      CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, true), this)
    CoreUtils.swallow(lazyOffsetIndex.close(), this)
    CoreUtils.swallow(lazyTimeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers(): Unit = {
    CoreUtils.swallow(lazyOffsetIndex.closeHandler(), this)
    CoreUtils.swallow(lazyTimeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists(): Unit = {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(lazyOffsetIndex.deleteIfExists _, "offset index", lazyOffsetIndex.file, logIfMissing = true),
      () => delete(lazyTimeIndex.deleteIfExists _, "time index", lazyTimeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  def deleted(): Boolean = {
    !log.file.exists() && !lazyOffsetIndex.file.exists() && !lazyTimeIndex.file.exists() && !txnIndex.file.exists()
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains, if maxTimestampSoFar >= 0, otherwise None.
   */
  def largestRecordTimestamp: Option[Long] = if (maxTimestampSoFar >= 0) Some(maxTimestampSoFar) else None

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyOffsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyTimeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    val maxIndexSize = config.maxIndexSize
    new LogSegment(
      FileRecords.open(UnifiedLog.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      LazyIndex.forOffset(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
      LazyIndex.forTime(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
      new TransactionIndex(baseOffset, UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      time)
  }

  def deleteIfExists(dir: File, baseOffset: Long, fileSuffix: String = ""): Unit = {
    UnifiedLog.deleteFileIfExists(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix))
    UnifiedLog.deleteFileIfExists(UnifiedLog.logFile(dir, baseOffset, fileSuffix))
  }
}

object LogFlushStats {
  private val metricsGroup = new KafkaMetricsGroup(LogFlushStats.getClass)
  val logFlushTimer: Timer = metricsGroup.newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
}