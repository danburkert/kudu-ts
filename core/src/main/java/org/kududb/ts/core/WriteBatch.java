package org.kududb.ts.core;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.SortedMap;
import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.ExternalConsistencyMode;
import org.kududb.client.KuduSession;
import org.kududb.client.OperationResponse;
import org.kududb.client.RowErrorsAndOverflowStatus;
import org.kududb.client.SessionConfiguration;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public class WriteBatch implements SessionConfiguration {
  private final KuduSession session;
  private final Metrics metrics;
  private final Tagsets tagsets;

  WriteBatch(KuduTS kuduTS) {
    this.session = kuduTS.getClient().syncClient().newSession();
    this.metrics = kuduTS.getMetrics();
    this.tagsets = kuduTS.getTagsets();
  }

  public OperationResponse writeDatapoint(final String metric,
                                          SortedMap<String, String> tags,
                                          final long time,
                                          final double value) throws Exception {
    int tagsetID = tagsets.getTagsetID(tags)
                          .joinUninterruptibly(session.getTimeoutMillis());
    return session.apply(metrics.insertDatapoint(metric, tagsetID, time, value));
  }

  /**
   * Blocking call that force flushes this batch's buffers. Data is persisted
   * when this call returns, else it will throw an exception.
   * @return a list of OperationResponse, one per datapoint that was flushed
   * @throws Exception if anything went wrong. If it's an issue with some or all batches,
   * it will be of type DeferredGroupException.
   */
  public List<OperationResponse> flush() throws Exception {
    return session.flush();
  }

  /**
   * Blocking call that flushes the buffers (see {@link #flush} and closes the batch.
   * @return List of OperationResponse, one per datapoint that was flushed
   * @throws Exception if anything went wrong. If it's an issue with some or all batches,
   * it will be of type DeferredGroupException.
   */
  public List<OperationResponse> close() throws Exception {
    return session.close();
  }

  /** {@inheritDoc} */
  @Override
  public FlushMode getFlushMode() {
    return session.getFlushMode();
  }

  /** {@inheritDoc} */
  @Override
  public void setFlushMode(FlushMode flushMode) {
    session.setFlushMode(flushMode);
  }

  /** {@inheritDoc} */
  @Override
  public void setMutationBufferSpace(int size) {
    session.setMutationBufferSpace(size);
  }

  /** {@inheritDoc} */
  @Override
  public void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage) {
    session.setMutationBufferLowWatermark(mutationBufferLowWatermarkPercentage);
  }

  /** {@inheritDoc} */
  @Override
  public void setFlushInterval(int interval) {
    session.setFlushInterval(interval);
  }

  /** {@inheritDoc} */
  @Override
  public long getTimeoutMillis() {
    return session.getTimeoutMillis();
  }

  /** {@inheritDoc} */
  @Override
  public void setTimeoutMillis(long timeout) {
    session.setTimeoutMillis(timeout);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isClosed() {
    return session.isClosed();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasPendingOperations() {
    return session.hasPendingOperations();
  }

  /** {@inheritDoc} */
  @Override
  public void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode) {
    session.setExternalConsistencyMode(consistencyMode);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isIgnoreAllDuplicateRows() {
    return session.isIgnoreAllDuplicateRows();
  }

  /** {@inheritDoc} */
  @Override
  public void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows) {
    session.setIgnoreAllDuplicateRows(ignoreAllDuplicateRows);
  }

  /** {@inheritDoc} */
  @Override
  public int countPendingErrors() {
    return session.countPendingErrors();
  }

  /** {@inheritDoc} */
  @Override
  public RowErrorsAndOverflowStatus getPendingErrors() {
    return session.getPendingErrors();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }
}
