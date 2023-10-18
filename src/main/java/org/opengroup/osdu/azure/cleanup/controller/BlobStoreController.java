package org.opengroup.osdu.azure.cleanup.controller;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import org.opengroup.osdu.azure.cleanup.config.BlobConfig;
import org.opengroup.osdu.azure.cleanup.records.CosmosResponseSchemaObject;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.springframework.beans.factory.annotation.Autowired;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.stream.Collectors;

@Repository
public class BlobStoreController {

    @Autowired
    private BlobConfig config;
    private static BlobServiceClient blobServiceClient;

    private BlobBatchClient getBlobBatchClient(){
        if(blobServiceClient == null){
            RequestRetryOptions requestRetryOptions = new RequestRetryOptions();
            StorageSharedKeyCredential storageSharedKeyCredential = new StorageSharedKeyCredential(config.getStorageAccountName(), config.getStorageAccountKey());

            blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(
                                    "https://%s.blob.core.windows.net", config.getStorageAccountName()))
                    .credential(storageSharedKeyCredential)
                    .retryOptions(requestRetryOptions)
                    .buildClient();
        }
        return new BlobBatchClientBuilder(blobServiceClient).buildClient();
    }
    private String getBlobUrlFromSchemaId(String schemaId){
        return String.format(
                "https://%s.blob.core.windows.net/opendes/%s.json", config.getStorageAccountName(), schemaId);
    }
    public void bulkDeleteSchemaBlobItems(List<CosmosResponseSchemaObject> schemaIdsToDelete) {
        List<String> blobUrls = schemaIdsToDelete
                .stream()
                .parallel()
                .map(schemaObject -> getBlobUrlFromSchemaId(schemaObject.id()))
                .collect(Collectors.toList());

        getBlobBatchClient().deleteBlobs(blobUrls, DeleteSnapshotsOptionType.INCLUDE)
                .forEach(response -> {
                    System.out.printf("Deleting blob with URL %s completed with status code %d%n",
                            response.getRequest().getUrl(), response.getStatusCode());
                });
    }
}
