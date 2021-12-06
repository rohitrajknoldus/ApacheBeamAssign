package com.Question2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MallCustomers {

    private static final String CSV_HEADER = "CustomerID,Genre,Age,Annual Income (k$)";

    public static void main(String[] args) {


        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[1], Double.parseDouble(tokens[3]));
                        }))
                .apply("MaxValue", Mean.perKey())
               // .apply(new StringBuilder().)
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + "," + productCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
<<<<<<< HEAD
                        .withHeader("gendre,avg_annual_income(k$),avg_spending_score"));
=======
                        .withHeader("Gender, Average"));
>>>>>>> e74523f4ebf0947f69b9bc2991036502a0dd5dc5

        pipeline.run();
        System.out.println("pipeline executed successfully");
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
