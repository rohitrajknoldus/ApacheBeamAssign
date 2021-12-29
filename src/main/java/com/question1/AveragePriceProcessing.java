package com.question1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import static com.sun.xml.internal.ws.spi.db.BindingContextFactory.LOGGER;

class AveragePriceProcessing {
    /**
     */
    private static final String CSV_HEADER = "Transaction_date,"
            + "Product,Price,Payment_Type,Name,City,State,Country,"
            + "Account_Created,Last_Login,Latitude,Longitude,US Zip";

    public static void main(final String[] args) {
        final AveragePriceProcessingOptions averagePriceProcessingOptions
                = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        pipeline.apply("Read-Lines", TextIO.read()
                .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                                !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[6] + "," + tokens[5],
                                    Double.parseDouble(tokens[2]));
                        }))
                .apply("MaxValue", Max.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + ","
                                + "," + productCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("State, city, max_price"));

        pipeline.run();
        LOGGER.info("pipeline executed successfully");
    }
    public interface AveragePriceProcessingOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/payment_type_count")
        String getOutputFile();
        void setOutputFile(String value);
    }
}
