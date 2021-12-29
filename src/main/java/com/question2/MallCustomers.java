package com.question2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.LoggerFactory;
import java.util.logging.Logger;

class MallCustomers {
/**
*LOGGER initializer as private static final
 */
    private static final Logger LOGGER
            = (Logger) LoggerFactory.getLogger(MallCustomers.class);
    /**
    * Main method called
     */
    private static final String CSV_HEADER = "CustomerID,"
            + "Genre,Age,Annual Income (k$)";

    public static void main(final String[] args) {
        AveragePriceProcessingOptions
                averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);
        
        // applied pipeline for the required file
        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[1],
                                    Double.parseDouble(tokens[3]));
                        }))
                .apply("MaxValue", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + ","
                                + productCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding().withSuffix(".csv"));
        
        LOGGER.info("pipeline executed successfully");// output message
        pipeline.run();// run pipeline required file
    }
    public interface AveragePriceProcessingOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/Mall_Customers_Income.csv")
        String getInputFile();
        void setInputFile(String value);
        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/MallCust")
        String getOutputFile();
        void setOutputFile(String value);
    }
}
