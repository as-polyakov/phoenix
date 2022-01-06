package org.apache.phoenix.query;

import static com.google.cloud.bigtable.hbase.adapters.Adapters.FILTER_ADAPTER;
import static com.google.cloud.bigtable.hbase.adapters.Adapters.ROW_RANGE_ADAPTER;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Query;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.repackaged.com.google.common.collect.RangeSet;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.cloud.bigtable.hbase.adapters.filters.BigtableFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.ColumnCountGetFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.ColumnPaginationFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.ColumnPrefixFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.ColumnRangeFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FamilyFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterListAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterSupportStatus;
import com.google.cloud.bigtable.hbase.adapters.filters.FirstKeyOnlyFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FuzzyRowFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.KeyOnlyFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.MultiRowRangeFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.MultipleColumnPrefixFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.PageFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.PrefixFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.QualifierFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.RandomRowFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.RowFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.SingleColumnValueExcludeFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.SingleColumnValueFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.SingleFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.TimestampRangeFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.TimestampsFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.TypedFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.TypedFilterAdapterBase;
import com.google.cloud.bigtable.hbase.adapters.filters.UnsupportedFilterException;
import com.google.cloud.bigtable.hbase.adapters.filters.UnsupportedStatusCollector;
import com.google.cloud.bigtable.hbase.adapters.filters.ValueFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.WhileMatchFilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.RowRangeAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ScanAdapter;
import com.google.cloud.bigtable.hbase.filter.BigtableFilter;
import com.google.cloud.bigtable.hbase.filter.TimestampRangeFilter;
import com.google.cloud.bigtable.hbase2_x.BigtableConnection;
import com.google.cloud.bigtable.hbase2_x.BigtableTable;
import com.google.protobuf.Service;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RowProcessor;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.coprocessor.CBTMetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.clientside.ClientSideObserver;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ServerCachingService;
import org.apache.phoenix.filter.SkipScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigTableConnectionFactory implements HConnectionFactory, HTableFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigTableConnectionFactory.class);
  private static ServerCachingEndpointImpl serverCachingEndpoint;

  public BigTableConnectionFactory() {
    serverCachingEndpoint = new ServerCachingEndpointImpl();
  }

  public static Region getFakeRegion() {
    return new Region() {
      @Override
      public RegionInfo getRegionInfo() {
        return null;
      }

      @Override
      public TableDescriptor getTableDescriptor() {
        return null;
      }

      @Override
      public boolean isAvailable() {
        return false;
      }

      @Override
      public boolean isClosed() {
        return false;
      }

      @Override
      public boolean isClosing() {
        return false;
      }

      @Override
      public boolean isReadOnly() {
        return false;
      }

      @Override
      public boolean isSplittable() {
        return false;
      }

      @Override
      public boolean isMergeable() {
        return false;
      }

      @Override
      public List<? extends Store> getStores() {
        return null;
      }

      @Override
      public Store getStore(byte[] family) {
        return null;
      }

      @Override
      public List<String> getStoreFileList(byte[][] columns) {
        return null;
      }

      @Override
      public boolean refreshStoreFiles() throws IOException {
        return false;
      }

      @Override
      public long getMaxFlushedSeqId() {
        return 0;
      }

      @Override
      public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
        return 0;
      }

      @Override
      public Map<byte[], Long> getMaxStoreSeqId() {
        return null;
      }

      @Override
      public long getEarliestFlushTimeForAllStores() {
        return 0;
      }

      @Override
      public long getReadRequestsCount() {
        return 0;
      }

      @Override
      public long getFilteredReadRequestsCount() {
        return 0;
      }

      @Override
      public long getWriteRequestsCount() {
        return 0;
      }

      @Override
      public long getMemStoreDataSize() {
        return 0;
      }

      @Override
      public long getMemStoreHeapSize() {
        return 0;
      }

      @Override
      public long getMemStoreOffHeapSize() {
        return 0;
      }

      @Override
      public long getNumMutationsWithoutWAL() {
        return 0;
      }

      @Override
      public long getDataInMemoryWithoutWAL() {
        return 0;
      }

      @Override
      public long getBlockedRequestsCount() {
        return 0;
      }

      @Override
      public long getCheckAndMutateChecksPassed() {
        return 0;
      }

      @Override
      public long getCheckAndMutateChecksFailed() {
        return 0;
      }

      @Override
      public void startRegionOperation() throws IOException {

      }

      @Override
      public void startRegionOperation(Operation op) throws IOException {

      }

      @Override
      public void closeRegionOperation() throws IOException {

      }

      @Override
      public void closeRegionOperation(Operation op) throws IOException {

      }

      @Override
      public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
        return null;
      }

      @Override
      public Result append(Append append) throws IOException {
        return null;
      }

      @Override
      public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException {
        return new OperationStatus[0];
      }

      @Override
      public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
          ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation)
          throws IOException {
        return false;
      }

      @Override
      public boolean checkAndMutate(byte[] row, Filter filter, TimeRange timeRange,
          Mutation mutation) throws IOException {
        return false;
      }

      @Override
      public boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier,
          CompareOperator op, ByteArrayComparable comparator, TimeRange timeRange,
          RowMutations mutations) throws IOException {
        return false;
      }

      @Override
      public boolean checkAndRowMutate(byte[] row, Filter filter, TimeRange timeRange,
          RowMutations mutations) throws IOException {
        return false;
      }

      @Override
      public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
        return null;
      }

      @Override
      public void delete(Delete delete) throws IOException {

      }

      @Override
      public Result get(Get get) throws IOException {
        return null;
      }

      @Override
      public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
        return null;
      }

      @Override
      public RegionScanner getScanner(Scan scan) throws IOException {
        return null;
      }

      @Override
      public RegionScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners)
          throws IOException {
        return null;
      }

      @Override
      public CellComparator getCellComparator() {
        return null;
      }

      @Override
      public Result increment(Increment increment) throws IOException {
        return null;
      }

      @Override
      public Result mutateRow(RowMutations mutations) throws IOException {
        return null;
      }

      @Override
      public void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock,
          long nonceGroup, long nonce) throws IOException {

      }

      @Override
      public void processRowsWithLocks(RowProcessor<?, ?> processor) throws IOException {

      }

      @Override
      public void processRowsWithLocks(RowProcessor<?, ?> processor, long nonceGroup, long nonce)
          throws IOException {

      }

      @Override
      public void processRowsWithLocks(RowProcessor<?, ?> processor, long timeout, long nonceGroup,
          long nonce) throws IOException {

      }

      @Override
      public void put(Put put) throws IOException {

      }

      @Override
      public CompactionState getCompactionState() {
        return null;
      }

      @Override
      public void requestCompaction(String why, int priority, boolean major,
          CompactionLifeCycleTracker tracker) throws IOException {

      }

      @Override
      public void requestCompaction(byte[] family, String why, int priority, boolean major,
          CompactionLifeCycleTracker tracker) throws IOException {

      }

      @Override
      public void requestFlush(FlushLifeCycleTracker tracker) throws IOException {

      }

      @Override
      public boolean waitForFlushes(long timeout) {
        return false;
      }

      @Override
      public void onConfigurationChange(Configuration conf) {

      }
    };
  }

  public static Configuration getFakeConfiguration() {
    return new Configuration(true);
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    LOGGER.info("Creating Cloud Bigtable connection");
    /*
    org.apache.hadoop.hbase.client.Connection c = BigtableConfiguration.connect(
        System.getenv("GCP_PROJECT_ID"),
        System.getenv("GCP_INSTANCE_ID"));
    */
    return new ExtendedBigtableConnection(BigtableConfiguration.configure(
        System.getenv("GCP_PROJECT_ID"),
        System.getenv("GCP_INSTANCE_ID")));
  }


  public static class CbtTable implements Table, InvocationHandler {

    final Table delegate;
    final Connection conneciton;

    public CbtTable(Table delegate, Connection conneciton) {
      this.delegate = delegate;
      this.conneciton = conneciton;
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
      ResultScanner underlyingResultScanner = delegate.getScanner(scan);
      RegionScanner overriddenRegionScanner = ClientSideObserver.getObserver(this)
          .postScannerOpen(getFakeObserverContext(), scan, new RegionScanner() {
            @Override
            public RegionInfo getRegionInfo() {
              return null;
            }

            @Override
            public boolean isFilterDone() throws IOException {
              return false;
            }

            @Override
            public boolean reseek(byte[] row) throws IOException {
              return false;
            }

            @Override
            public long getMaxResultSize() {
              return 0;
            }

            @Override
            public long getMvccReadPoint() {
              return 0;
            }

            @Override
            public int getBatch() {
              return 0;
            }

            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
              Result r = underlyingResultScanner.next();
              if (r == null) {
                return false;
              }
              result.addAll(r.listCells());
              return true;
            }

            @Override
            public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
              return nextRaw(result);
            }

            @Override
            public boolean next(List<Cell> result, ScannerContext scannerContext)
                throws IOException {
              return nextRaw(result);
            }

            @Override
            public void close() throws IOException {

            }
          });
      return new ResultScanner() {

        @Override
        public Result next() throws IOException {
          List<Cell> cellList = new ArrayList<>();
          if (overriddenRegionScanner.nextRaw(cellList)) {
            return Result.create(cellList);
          }
          return null;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean renewLease() {
          return false;
        }

        @Override
        public ScanMetrics getScanMetrics() {
          return null;
        }
      };
    }

    public static ObserverContext<RegionCoprocessorEnvironment> getFakeObserverContext() {
      return new ObserverContext<RegionCoprocessorEnvironment>() {

        @Override
        public RegionCoprocessorEnvironment getEnvironment() {
          return new RegionCoprocessorEnvironment() {
            @Override
            public Region getRegion() {
              return getFakeRegion();
            }

            @Override
            public RegionInfo getRegionInfo() {
              return null;
            }

            @Override
            public OnlineRegions getOnlineRegions() {
              return null;
            }

            @Override
            public ConcurrentMap<String, Object> getSharedData() {
              return null;
            }

            @Override
            public ServerName getServerName() {
              return null;
            }

            @Override
            public Connection getConnection() {
              return null;
            }

            @Override
            public Connection createConnection(Configuration conf) throws IOException {
              return null;
            }

            @Override
            public MetricRegistry getMetricRegistryForRegionServer() {
              return null;
            }

            @Override
            public RawCellBuilder getCellBuilder() {
              return null;
            }

            @Override
            public int getVersion() {
              return 0;
            }

            @Override
            public String getHBaseVersion() {
              return null;
            }

            @Override
            public RegionCoprocessor getInstance() {
              return null;
            }

            @Override
            public int getPriority() {
              return 0;
            }

            @Override
            public int getLoadSequence() {
              return 0;
            }

            @Override
            public Configuration getConfiguration() {
              return getFakeConfiguration();
            }

            @Override
            public ClassLoader getClassLoader() {
              return null;
            }
          };
        }

        @Override
        public void bypass() {

        }

        @Override
        public Optional<User> getCaller() {
          return Optional.empty();
        }
      };
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
        byte[] startKey, byte[] endKey, Call<T, R> callable) throws Throwable {
      R result = null;
      if (service == ServerCachingService.class) {
        serverCachingEndpoint.start(getFakeObserverContext().getEnvironment());
        result = callable.call((T) serverCachingEndpoint);
      } else if (service == MetaDataService.class) {
        result = callable.call((T) new CBTMetaDataEndpointImpl(conneciton));
      } else {
        throw new RuntimeException("Trying to invoke non-mocked coprocessor: " + service.getName());
      }

      HashedMap results = new HashedMap();
      results.put(new byte[0], result);
      return results;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Method m = findMethod(this.getClass(), method, false);
      if (m != null) {
        return m.invoke(this, args);
      }
      m = findMethod(delegate.getClass(), method, true);
      if (m != null) {
        return m.invoke(delegate, args);
      }
      throw new RuntimeException("No method found " + method);
    }

    private Method findMethod(Class<?> clazz, Method method, boolean includeSubclasses)
        throws Throwable {
      try {
        return includeSubclasses ? clazz.getMethod(method.getName(), method.getParameterTypes())
            : clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException e) {
        return null;
      }
    }

    @Override
    public TableName getName() {
      return delegate.getName();
    }

    @Override
    public Configuration getConfiguration() {
      return delegate.getConfiguration();
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
      return delegate.getDescriptor();
    }

    @Override
    public RegionLocator getRegionLocator() throws IOException {
      return null;
    }
  }

  @Override
  public Table getTable(byte[] tableName, Connection connection, ExecutorService pool)
      throws IOException {
    final Table t = connection.getTable(TableName.valueOf(tableName));
    return (Table) Proxy.newProxyInstance(CbtTable.class.getClassLoader(), new Class[]{Table.class},
        new CbtTable(t, connection));
  }


  public class ExtendedHBaseRequestAdapter extends HBaseRequestAdapter {

    public ExtendedHBaseRequestAdapter(
        BigtableOptions options,
        TableName tableName, Configuration config) {
      super(options, tableName, config);
    }

    public ExtendedHBaseRequestAdapter(
        BigtableOptions options, TableName tableName,
        MutationAdapters mutationAdapters) {
      super(options, tableName, mutationAdapters);
    }

    public Query adapt(Scan scan) {
      ReadHooks readHooks = new DefaultReadHooks();
      Query query = Query.create(this.bigtableTableName.getTableId());
      //Adapters.SCAN_ADAPTER.adapt(scan, readHooks, query);
      ScanAdapter scanAdapter = new ScanAdapter(ExtendedFilterAdapter.buildAdapter(),
          ROW_RANGE_ADAPTER);
      scanAdapter.adapt(scan, readHooks, query);
      readHooks.applyPreSendHook(query);
      return query;
    }
  }

  public class ExtendedScanAdapter extends ScanAdapter {

    public ExtendedScanAdapter(
        FilterAdapter filterAdapter,
        RowRangeAdapter rowRangeAdapter) {
      super(filterAdapter, rowRangeAdapter);
    }
  }

  public static class ExtendedFilterAdapter extends FilterAdapter {

    private Map<Class<? extends Filter>, SingleFilterAdapter<?>> adapterMap = new HashMap();


    public static FilterAdapter buildAdapter() {
      ExtendedFilterAdapter adapter = new ExtendedFilterAdapter();
      adapter.addFilterAdapter(SkipScanFilter.class, new SkipScanFilterAdapter());
      adapter.addFilterAdapter(ColumnPrefixFilter.class, new ColumnPrefixFilterAdapter());
      adapter.addFilterAdapter(ColumnRangeFilter.class, new ColumnRangeFilterAdapter());
      adapter.addFilterAdapter(KeyOnlyFilter.class, new KeyOnlyFilterAdapter());
      adapter.addFilterAdapter(
          MultipleColumnPrefixFilter.class, new MultipleColumnPrefixFilterAdapter());
      adapter.addFilterAdapter(TimestampsFilter.class, new TimestampsFilterAdapter());
      adapter.addFilterAdapter(TimestampRangeFilter.class, new TimestampRangeFilterAdapter());
      ValueFilterAdapter valueFilterAdapter = new ValueFilterAdapter();
      adapter.addFilterAdapter(ValueFilter.class, valueFilterAdapter);
      SingleColumnValueFilterAdapter scvfa = new SingleColumnValueFilterAdapter(valueFilterAdapter);
      adapter.addFilterAdapter(SingleColumnValueFilter.class, scvfa);
      adapter.addFilterAdapter(
          SingleColumnValueExcludeFilter.class, new SingleColumnValueExcludeFilterAdapter(scvfa));
      adapter.addFilterAdapter(ColumnPaginationFilter.class, new ColumnPaginationFilterAdapter());
      adapter.addFilterAdapter(FirstKeyOnlyFilter.class, new FirstKeyOnlyFilterAdapter());
      adapter.addFilterAdapter(ColumnCountGetFilter.class, new ColumnCountGetFilterAdapter());
      adapter.addFilterAdapter(RandomRowFilter.class, new RandomRowFilterAdapter());
      adapter.addFilterAdapter(PrefixFilter.class, new PrefixFilterAdapter());
      adapter.addFilterAdapter(QualifierFilter.class, new QualifierFilterAdapter());
      adapter.addFilterAdapter(PageFilter.class, new PageFilterAdapter());
      adapter.addFilterAdapter(WhileMatchFilter.class, new WhileMatchFilterAdapter(adapter));
      adapter.addFilterAdapter(RowFilter.class, new RowFilterAdapter());
      adapter.addFilterAdapter(FuzzyRowFilter.class, new FuzzyRowFilterAdapter());
      adapter.addFilterAdapter(FamilyFilter.class, new FamilyFilterAdapter());
      adapter.addFilterAdapter(BigtableFilter.class, new BigtableFilterAdapter());

      try {
        adapter.addFilterAdapter(MultiRowRangeFilter.class, new MultiRowRangeFilterAdapter());
      } catch (NoClassDefFoundError var4) {
      }

      FilterListAdapter filterListAdapter = new FilterListAdapter(adapter);
      adapter.addFilterAdapter(FilterList.class, filterListAdapter, filterListAdapter);
      return adapter;
    }

    protected SingleFilterAdapter<?> getAdapterForFilterOrThrow(Filter filter) {
      try {
        return super.getAdapterForFilterOrThrow(filter);
      } catch (UnsupportedFilterException e) {
        if (this.adapterMap.containsKey(filter.getClass())) {
          return (SingleFilterAdapter) this.adapterMap.get(filter.getClass());
        } else {
          throw new UnsupportedFilterException(
              ImmutableList.of(FilterSupportStatus.newUnknownFilterType(filter)));
        }
      }
    }

    private <T extends Filter> void addFilterAdapter(Class<T> filterType,
        TypedFilterAdapter<T> typedFilterAdapter, UnsupportedStatusCollector<T> collector) {
      this.adapterMap.put(filterType,
          new SingleFilterAdapter(filterType, typedFilterAdapter, collector));
    }

    private <T extends Filter> void addFilterAdapter(Class<T> filterType,
        TypedFilterAdapter<T> typedFilterAdapter) {
      this.adapterMap.put(filterType, new SingleFilterAdapter(filterType, typedFilterAdapter));
    }

    public void collectUnsupportedStatuses(FilterAdapterContext context, Filter filter,
        List<FilterSupportStatus> statuses) {
      List<FilterSupportStatus> innerUnsupportedList = new ArrayList<>(statuses);
      SingleFilterAdapter<?> adapter = (SingleFilterAdapter) this.adapterMap.get(filter.getClass());
      if (adapter == null) {
        statuses.add(FilterSupportStatus.newUnknownFilterType(filter));
      } else {
        adapter.collectUnsupportedStatuses(context, filter, statuses);
      }
      /*
      super.collectUnsupportedStatuses(context, filter, innerUnsupportedList);
      List<FilterSupportStatus> filteredList = innerUnsupportedList.stream()
          .filter(f -> !hasAdapterForFilter(parseClassName(f.toString()))).collect(
              Collectors.toList());
      if (adapterMap.containsKey(filter.getClass())) {
        this.adapterMap.get(filter.getClass())
            .collectUnsupportedStatuses(context, filter, filteredList);
      }
      statuses.addAll(filteredList);
      */
    }

    private boolean hasAdapterForFilter(String filterClassName) {
      try {
        Class<?> clazz = Class.forName(filterClassName);
        return this.adapterMap.containsKey(clazz);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("This should never happen", e);
      }
    }

    private String parseClassName(String msg) {
      //I am not proud of it, but I have no time
      return msg.split(" ")[7];
    }
  }

  public static class SkipScanFilterAdapter extends TypedFilterAdapterBase<SkipScanFilter> {

    @Override
    public Filters.Filter adapt(FilterAdapterContext filterAdapterContext,
        SkipScanFilter skipScanFilter) throws IOException {
      String regexValue = ".*";
      return Filters.FILTERS.key().regex(regexValue);
    }

    @Override
    public FilterSupportStatus isFilterSupported(FilterAdapterContext filterAdapterContext,
        SkipScanFilter skipScanFilter) {
      return FilterSupportStatus.SUPPORTED;
    }
  }

  public class ExtendedBigtableConnection extends BigtableConnection {

    public ExtendedBigtableConnection(Configuration conf) throws IOException {
      super(conf);
    }

    public ExtendedBigtableConnection(Configuration conf, ExecutorService pool,
        User user) throws IOException {
      super(conf, pool, user);
    }

    public Table getTable(TableName tableName, ExecutorService ignored) throws IOException {
      return new BigtableTable(this, this.createAdapter(tableName));
    }

    public HBaseRequestAdapter createAdapter(TableName tableName) {
      super.createAdapter(tableName);
      return new ExtendedHBaseRequestAdapter(super.getOptions(), tableName,
          super.getConfiguration());
    }
  }
}