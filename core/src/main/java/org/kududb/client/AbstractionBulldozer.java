package org.apache.kudu.client;

public final class AbstractionBulldozer {

  private AbstractionBulldozer() {}

  /**
   * Set {@link AbstractKuduScannerBuilder#sortResultsByPrimaryKey} on a scanner builder.
   *
   * @param builder to set primary key sorting on
   * @return the builder
   */
  public static <S extends AbstractKuduScannerBuilder<S, T>, T> AbstractKuduScannerBuilder<S, T>
  sortResultsByPrimaryKey(AbstractKuduScannerBuilder<S, T> builder) {
    builder.sortResultsByPrimaryKey();
    return builder;
  }
}
