package dev.k1k.batchprocessor.dynamo;

import dev.k1k.batchprocessor.model.OutputRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import java.util.List;

import static dev.k1k.batchprocessor.dynamo.DynamoBatchWriteUtil.batchWrite;

@Component
@RequiredArgsConstructor
public class DynamoDBWriter {
    private final DynamoDbEnhancedClient dynamoDbEnhancedClient = null;

    public void putBatchRecords(List<OutputRecord> objects) {
        DynamoDbTable<OutputRecord> outputMappedTable = dynamoDbEnhancedClient.table("output", TableSchema.fromBean(OutputRecord.class));
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_dynamodb_BatchWriteItem_section.html

        batchWrite(OutputRecord.class, objects, dynamoDbEnhancedClient, outputMappedTable);
    }


}
