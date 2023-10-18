package org.opengroup.osdu.azure.cleanup.controller;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import org.opengroup.osdu.azure.cleanup.config.CosmosConfig;
import org.opengroup.osdu.azure.cleanup.config.SchemaQueryParameters;
import org.opengroup.osdu.azure.cleanup.records.CosmosResponseSchemaObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CosmosDbController {
    private static CosmosClient cosmosClient;
    @Autowired
    private CosmosConfig config;
    @Autowired
    private SchemaQueryParameters queryParameters;

    public CosmosContainer getCosmosContainer(){
        if(cosmosClient == null)
        {
            cosmosClient = new CosmosClientBuilder()
                    .endpoint(config.getCosmosHost())
                    .key(config.getCosmosKey())
                    .buildClient();
        }

        return cosmosClient.getDatabase("osdu-db").getContainer("SchemaInfo");
    }

    public List<CosmosResponseSchemaObject> GetRecordsToDelete(){
        List<CosmosResponseSchemaObject> filteredRecords = new ArrayList<>();
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        String selectQuery = String.format("SELECT c.id, c.partitionId FROM c WHERE CONTAINS(c.id, \"%s\")", queryParameters.getSchemaIdContainsQueryParam());
        getCosmosContainer().queryItems(selectQuery, queryOptions, CosmosResponseSchemaObject.class).
        forEach(item -> filteredRecords.add(item));
        return filteredRecords;
    }

    public void bulkDeleteItems(List<CosmosResponseSchemaObject> listToDelete) {
        List<CosmosItemOperation> itemOperations = new ArrayList<>();
        listToDelete.stream().parallel().forEach(schemaObject -> {
            System.out.println(schemaObject.id());
            itemOperations.add(CosmosBulkOperations
                    .getDeleteItemOperation(schemaObject.id(), new PartitionKey(schemaObject.partitionId())));
        });
        if (itemOperations.size() > 0) {
            getCosmosContainer().executeBulkOperations(itemOperations).forEach(response ->{
                System.out.printf("Deleting blob with URL %s completed with status code %d%n",
                        response.getOperation().getId(), response.getResponse().getStatusCode());
            });
        }
    }
}
