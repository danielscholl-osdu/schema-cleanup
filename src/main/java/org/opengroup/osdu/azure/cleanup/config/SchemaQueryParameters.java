package org.opengroup.osdu.azure.cleanup.config;

import com.azure.core.annotation.Get;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class SchemaQueryParameters {
    @Value("${schema_id_query_param}")
    private String schemaIdContainsQueryParam;
}
