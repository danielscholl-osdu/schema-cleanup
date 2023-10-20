package org.opengroup.osdu.azure.cleanup.repository;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import lombok.extern.slf4j.Slf4j;
import org.opengroup.osdu.azure.cleanup.config.CosmosConfig;
import org.opengroup.osdu.azure.cleanup.config.SchemaQueryParameters;
import org.opengroup.osdu.azure.cleanup.records.CosmosResponseSchemaObject;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class CosmosDbRepository {
    private CosmosClient cosmosClient;
    @Autowired
    private CosmosConfig config;
    @Autowired
    private SchemaQueryParameters queryParameters;

    public CosmosContainer getCosmosContainer() {
        if (cosmosClient == null)
            cosmosClient = new CosmosClientBuilder()
                    .endpoint(config.getCosmosHost())
                    .key(config.getCosmosKey())
                    .buildClient();

        return cosmosClient.getDatabase("osdu-db").getContainer("SchemaInfo");
    }

    public List<CosmosResponseSchemaObject> getRecordsToDelete() {
        try {
            List<CosmosResponseSchemaObject> filteredRecords = new ArrayList<>();
            CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
            String selectQuery = String.format("SELECT c.id, c.partitionId FROM c WHERE CONTAINS(c.id, \"%s\")", queryParameters.getId());
            getCosmosContainer().queryItems(selectQuery, queryOptions, CosmosResponseSchemaObject.class).
                    forEach(filteredRecords::add);
            return filteredRecords;
        } catch (CosmosException e) {
            throw new AppException(e.getStatusCode(), "Exception while fetching records from the cosmos", e.getMessage());
        }
    }

    public void bulkDeleteItems(List<CosmosResponseSchemaObject> listToDelete) {
        List<CosmosItemOperation> itemOperations = new ArrayList<>();
        listToDelete.stream().parallel().forEach(schemaObject ->
                itemOperations.add(CosmosBulkOperations
                        .getDeleteItemOperation(schemaObject.id(), new PartitionKey(schemaObject.partitionId()))
                ));
        if (!itemOperations.isEmpty())
            getCosmosContainer().executeBulkOperations(itemOperations).forEach(response ->
                    log.info(String.format("Deleting schema %s completed with status code %d%n",
                            response.getOperation().getId(), response.getResponse().getStatusCode())
                    )
            );
    }
}
