package org.apache.phoenix.coprocessor.clientside;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class ClientSideObserver {

  public static RegionObserver getObserver(Table t) {
    if (t.getName().isSystemTable() || t.getName().toString().toUpperCase().startsWith("SYSTEM")) {
      return new RegionObserver() {
        @Override
        public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
            Scan scan, RegionScanner s) throws IOException {
          return s;
        }
      };
    }
    return new ScanRegionObserver();
  }
}
