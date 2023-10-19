package org.opengroup.osdu.azure.cleanup.security;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

@Component("authorizationFilter")
@RequestScope
public class AuthorizationFilter {

    @Autowired
    private DpsHeaders headers;

    @Autowired
    private IAuthorizationServiceForServiceAdmin authorizationService;

    public boolean hasPermissions() {
        headers.put(DpsHeaders.USER_EMAIL, "ServiceAdminUser");
        return authorizationService.isDomainAdminServiceAccount();
    }

}