package dev.k1k.batchprocessor.dynamo;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DynamoBatchWriteUtil {
    // https://stackoverflow.com/a/68217317 Write more than 25 items using BatchWriteItemEnhancedRequest Dynamodb JAVA SDK 2
    // https://stackoverflow.com/a/50646576 Efficient way to divide a list into lists of n size
    private static final int MAX_DYNAMODB_BATCH_SIZE = 25;  // AWS doesn't like if you try to include more than 25 items in a batch or sub-batch

    /**
     * Writes the list of items to the specified DynamoDB table.
     */
    public static <T> void batchWrite(Class<T> itemType, List<T> items, DynamoDbEnhancedClient client, DynamoDbTable<T> table) {
        Stream<List<T>> chunksOfItems = partition(items).stream();
        chunksOfItems.forEach(chunkOfItems -> {
            List<T> unprocessedItems = batchWriteImpl(itemType, chunkOfItems, client, table);
            while (!unprocessedItems.isEmpty()) {
                // some failed (provisioning problems, etc.), so write those again
                unprocessedItems = batchWriteImpl(itemType, unprocessedItems, client, table);
            }
        });
    }

    /**
     * Writes a single batch of (at most) 25 items to DynamoDB.
     * Note that the overall limit of items in a batch is 25, so you can't have nested batches
     * of 25 each that would exceed that overall limit.
     *
     * @return those items that couldn't be written due to provisioning issues, etc., but were otherwise valid
     */
    private static <T> List<T> batchWriteImpl(Class<T> itemType, List<T> chunkOfItems, DynamoDbEnhancedClient client, DynamoDbTable<T> table) {
        WriteBatch.Builder<T> subBatchBuilder = WriteBatch.builder(itemType).mappedTableResource(table);
        chunkOfItems.forEach(subBatchBuilder::addPutItem);

        BatchWriteItemEnhancedRequest.Builder overallBatchBuilder = BatchWriteItemEnhancedRequest.builder();
        overallBatchBuilder.addWriteBatch(subBatchBuilder.build());

        return client.batchWriteItem(overallBatchBuilder.build())
                .unprocessedPutItemsForTable(table);
    }

    private static <T> List<List<T>> partition(List<T> objs) {
        return new ArrayList<>(
                IntStream.range(0, objs.size())
                        .boxed()
                        .collect(Collectors.groupingBy(e -> e / MAX_DYNAMODB_BATCH_SIZE, Collectors.mapping(objs::get, Collectors.toList())))
                        .values());
    }
}
