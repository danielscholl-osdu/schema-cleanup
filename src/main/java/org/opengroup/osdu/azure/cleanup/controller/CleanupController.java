package org.opengroup.osdu.azure.cleanup.controller;
import org.opengroup.osdu.azure.cleanup.records.CleanupResponse;
import org.opengroup.osdu.azure.cleanup.service.CleanupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("schema-cleanup")
public class CleanupController {
    @Autowired
    private CleanupService cleanupService;
    @PostMapping("/clean")
    //@PreAuthorize("@authorizationFilter.hasPermissions()")
    public ResponseEntity<CleanupResponse> cleanupSchemas(){
        CleanupResponse cleanupResponse = cleanupService.cleanupRecords();
        ResponseEntity<CleanupResponse> response = new ResponseEntity<>(cleanupResponse, cleanupResponse.statusCode());
        return response;
    }

}
