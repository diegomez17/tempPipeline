import com.google.api.services.bigquery.model.BigtableOptions;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStub;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CallablePipeline {

  private static final String tableId = "testTable";
  private static final byte[] FAMILY_BYTES = Bytes.toBytes("cf1");
  private static final byte[] QUAL_BYTES = Bytes.toBytes("qual");

  public static void main(String[] args) {
    int numRows = 100000000;

    String[] intro = new String[8];
    intro[0] = "--bigtableProjectId=google.com:cloud-bigtable-dev";
    intro[1] = "--bigtableInstanceId=diegomez-dev";
    intro[2] = "--bigtableTableId=testTable";
    intro[3] = "--gcpTempLocation=gs://diegomez-dataflow/temp";
    intro[4] = "--runner=dataflow";
    intro[5] = "--region=us-east1";
    intro[6] = "--numWorkers=1";
    intro[7] = "--autoscalingAlgorithm=NONE";

    BigtableOptions options =
        PipelineOptionsFactory.fromArgs(intro).withValidation().as(BigtableOptions.class);

    Pipeline p = Pipeline.create(options);

    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());

    CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(options.getProject())
        .withInstanceId(options.getBigtableInstanceId())
        .withTableId(tableId)
        .build();

    //long starting_timestamp = System.currentTimeMillis();
    p.apply(GenerateSequence.from(0).to(numRows))
        .apply(ParDo.of(
            new DoFn<Long, Mutation>() {
              long starting_timestamp = System.currentTimeMillis();

              @ProcessElement
              public void processElement(DoFn<Long, Mutation>.ProcessContext c)
                  throws Exception {

                Long numberKey = c.element();
                long thread_timestamp = System.currentTimeMillis();

                Put p = new Put(Bytes.toBytes(numberKey));
                p.addColumn(
                    FAMILY_BYTES,
                    QUAL_BYTES,
                    Bytes.toBytes(numberKey)
                );


                c.output(p);
              }
            })).apply(CloudBigtableIO.writeToTable(config));

    p.run().waitUntilFinish();
  }

  // Pipeline options
  public interface BigtableOptions extends DataflowPipelineOptions {
    String getBigtableProjectId();

    void setBigtableProjectId(String bigtableProjectId);

    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);

    String getBqQuery();

    void setBqQuery(String value);
  }
}

