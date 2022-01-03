package org.apache.phoenix.query;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.protobuf.Service;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.coprocessor.CBTMetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.clientside.ClientSideObserver;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;

public class BigTableConnectionFactory implements HConnectionFactory, HTableFactory {

  public BigTableConnectionFactory() {
  }

  public static Region getFakeRegion() {
      return null;
  }

  public static Configuration getFakeConfiguration() {
      return new Configuration(true);
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    org.apache.hadoop.hbase.client.Connection c = BigtableConfiguration.connect(System.getenv("GCP_PROJECT_ID"),
        System.getenv("GCP_INSTANCE_ID"));
  return c;
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
          if(r == null)
            return false;
          result.addAll(r.listCells());
          return true;
        }

        @Override
        public boolean nextRaw(List<Cell> result, ScannerContext scannerContext)
            throws IOException {
          return nextRaw(result);
        }

        @Override
        public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
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
          if(overriddenRegionScanner.nextRaw(cellList)) {
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
      if(service != MetaDataService.class)
        throw new RuntimeException("Trying to invoke non-mocked coprocessor: " + service.getName());
      R result = callable.call((T) new CBTMetaDataEndpointImpl(conneciton));

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

    private Method findMethod(Class<?> clazz, Method method, boolean includeSubclasses) throws Throwable {
      try {
        return includeSubclasses ? clazz.getMethod(method.getName(), method.getParameterTypes()) : clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
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
    return (Table) Proxy.newProxyInstance(CbtTable.class.getClassLoader(), new Class[] { Table.class }, new CbtTable(t, connection));
  }
}
