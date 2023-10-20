package org.opengroup.osdu.azure.cleanup.service;

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.guava25.collect.Lists;
import com.azure.storage.blob.models.BlobStorageException;
import org.opengroup.osdu.azure.cleanup.records.CleanupResponse;
import org.opengroup.osdu.azure.cleanup.records.CosmosResponseSchemaObject;
import org.opengroup.osdu.azure.cleanup.repository.BlobStoreRepository;
import org.opengroup.osdu.azure.cleanup.repository.CosmosDbRepository;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CleanupService {
    @Autowired
    private CosmosDbRepository cosmosDbRepository;
    @Autowired
    private BlobStoreRepository blobStoreRepository;

    public CleanupResponse cleanupRecords() {
        List<CosmosResponseSchemaObject> schemasToBeDeleted  = cosmosDbRepository.getRecordsToDelete();
        List<String> schemaIdsDeleted = new ArrayList<>();
        if (!schemasToBeDeleted.isEmpty()) {
            deleteFromCosmosInBatches(schemasToBeDeleted);
            deleteFromBlobStorageInBatches(schemasToBeDeleted);
            schemasToBeDeleted.stream()
                    .parallel()
                    .forEach(item -> schemaIdsDeleted.add(item.id()));
        }
        return new CleanupResponse(schemaIdsDeleted.size(), schemaIdsDeleted, HttpStatus.OK);
    }

    private void deleteFromBlobStorageInBatches(List<CosmosResponseSchemaObject> schemasToBeDeleted) {
        try {
            List<List<CosmosResponseSchemaObject>> listsToDelete = Lists.partition(schemasToBeDeleted, 100);
            for (List<CosmosResponseSchemaObject> listToDelete : listsToDelete) {
                blobStoreRepository.bulkDeleteSchemaBlobItems(listToDelete);
            }
        } catch (BlobStorageException e) {
            throw new AppException(e.getStatusCode(), "Exception while deleting the blob", e.getMessage());
        } catch (Exception e) {
            throw new AppException(500, "Unknown Exception while deleting the blob", "An unknown Exception occurred while deleting the blob: " + e.getMessage());
        }
    }

    private void deleteFromCosmosInBatches(List<CosmosResponseSchemaObject> schemasToBeDeleted) {
        try {
            List<List<CosmosResponseSchemaObject>> listsToDelete = Lists.partition(schemasToBeDeleted, 100);
            for (List<CosmosResponseSchemaObject> listToDelete : listsToDelete) {
                cosmosDbRepository.bulkDeleteItems(listToDelete);
            }
        } catch (CosmosException e) {
            throw new AppException(e.getStatusCode(), "Exception while deleting from the cosmos", e.getMessage());
        }
    }
}
