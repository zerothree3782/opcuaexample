package com.hhi.opcuaexample.properties;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;

import java.util.function.Predicate;

@Getter
@Setter
public class ClientProperties {

    String endpointUrl;

    Predicate<EndpointDescription> endpointFilter;

    SecurityPolicy securityPolicy;

    IdentityProvider identityProvider;

}
