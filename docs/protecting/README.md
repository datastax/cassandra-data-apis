# Controlling and managing access to your GraphQL API

There are several ways you can use to protect your GraphQL API. While supporting built-in authentication
mechanisms and other access restriction methods are in the project roadmap, these features are not yet implemented.
We recommend that you consider one of the following strategies depending on your deployment model
and requirements: you can use an existing cloud service, deploy a reverse proxy server like
[Envoy][envoy] / [NGINX][nginx] or use a service mesh ingress controller like [Istio Gateway][istio-gateway].

## Using a cloud service

Cloud providers offer API gateway services that can help you manage access to your GraphQL API. For example:

- [Amazon API Gateway][api-gateway-aws]
- [Google Cloud Endpoints][api-gateway-gce]
- [Azure API Management][api-gateway-azure]

Using API gateway services you can control how your GraphQL endpoint is accessed, allowing access to your
API from selected VPCs and from selected accounts.

### Example: Creating a private API in Amazon API Gateway

Deploy the GraphQL endpoint on a Virtual Private Cloud (VPC) subnet, within the same VPC as the
Apache Cassandra nodes or within a [peered VPC][aws-vpc-peering]. Make sure you only allow ingress from desired
CIDR blocks to the [security group][aws-security-groups]. Also, don't associate public IP addresses to the endpoint.

[Create a private Amazon API Gateway][aws-private-api] that can only be accessed from your virtual private
cloud by assigning the VPC endpoint id(s). Next, [create a child resource, marking it as a
"proxy resource"][aws-proxy-integration], set up integration type as HTTP proxy and enter the endpoint URL.

You can now control access to your API Gateway by managing the subnets and security groups for the VPC endpoint.

## Using a reverse proxy server

You can use a L7 proxy server like [Envoy][envoy] or [NGINX][nginx] to delegate access control and http connection
management, allowing you to place your GraphQL API instance under a private subnet. You can then protect your proxy
instance with http filters, integrating it with your own authorization mechanisms.

### Envoy

Envoy can be used to control access to the data APIs using an [external authentication
service](#external-authentication), [JSON Web Tokens (JWT)](#json-web-tokens-jwt), or [TLS client
certificates](#client-certificates). 

Start with a basic proxy forwarding:

``` yaml
static_resources:
  listeners:
  - name: listener0
    address:
      socket_address: { address: 0.0.0.0, port_value: 10000 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager
          stat_prefix: ingress_http
          codec_type: AUTO
          route_config:
            name: local_route
            virtual_hosts:
            - name: cassandra_apis
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: cassandra_apis }
          http_filters:
          - name: envoy.filters.http.router

  clusters:
  - name: cassandra_apis
    connect_timeout: 0.25s
    type: STATIC
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: cassandra_apis
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: 127.0.0.1
                port_value: 8080

```

#### External Authentication

External authentication forwards a subrequest with select headers to the external service for
validation. More information can be found in the [Envoy external authentication
documentation][envoy-ext-auth].

```yaml
static_resources:
  listeners:
  - name: listener0
    # ...
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager
          # ...
          http_filters:
          - name: envoy.ext_authz
            typed_config:
              "@type": type.googleapis.com/envoy.config.filter.http.ext_authz.v2.ExtAuthz
              http_service:
                server_uri:
                  uri: 127.0.0.1:8081
                  cluster: your_auth_service
                  timeout: 0.25s
                authorization_request:
                  allowed_headers: 
                    patterns:
                      exact: "Authorization"
                      # ...
                authorization_response:
                  allowed_upstream_headers: 
                    patterns:
                      exact: "Authorization"
                      # ...
                  allowed_client_headers: 
                    patterns:
                      exact: "Authorization"
                      # ...
          - name: envoy.filters.http.router

  clusters:
  # ...
  - name: your_auth_service
    connect_timeout: 0.25s
    type: STATIC
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: your_auth_service
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: 127.0.0.1
                port_value: 8081
```

#### JSON Web Tokens (JWT)

JWT authentication validates JSON web tokens against keys provided by either a local file or a
remote provider. More information can be found in the [Envoy JWT documentation][envoy-jwt].

```yaml
static_resources:
  listeners:
  - name: listener0
    # ...
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager
          # ...
          http_filters:
          - name: envoy.filters.http.jwt_authn
            typed_config: 
              "@type": type.googleapis.com/envoy.config.filter.http.jwt_authn.v2alpha.JwtAuthentication
              providers:
                auth0:
                  issuer: https://your-tokens-issuer.com/
                  audiences:
                  - https://your-services-url.com
                  remote_jwks:
                    http_uri:
                      uri: 127.0.0.1:8081
                      cluster: jwt_auth
                      timeout: 5s
                    cache_duration:
                      seconds: 300
          - name: envoy.filters.http.router

  clusters:
  # ...
  - name: jwt_auth
    connect_timeout: 0.25s
    type: STATIC
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: jwt_auth
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: 127.0.0.1
                port_value: 8081
```

#### Client Certificates

Clients can be verified and authenticated server-side using TLS along with a certificate provided
by the client. More information about enabling client certificates and TLS can be found in the
[Envoy documentation][envoy-tls].


```yaml
static_resources:
  listeners:
  - name: listener0
    # ...
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager
          # ...
      tls_context:
        common_tls_context:
          require_client_certificate: true
          validation_context:
            trusted_ca:
              filename: "/path/to/ca.pem"
            verify_certificate_hash:
              # Hashes for authorized client certificates
              - "068ec9830b228c69869babd5c4c15e346d84dba17f414de0460bd0901b2ca8af"
              # ...
          tls_certificates:
            - certificate_chain:
                filename: "/path/to/cert.pem"
              private_key:
                filename: "/path/to/key.pem"

  clusters:
  # ...
```

### NGNIX

NGNIX can also be used control access using either an [external authentication
service](#external-authentication-1) or [TLS client certificates](#client-certificates-1). 

Start with a basic proxy forwarding:

```
server {
    listen 10000;

    location / {
       proxy_pass http://127.0.0.1:8080/;
    }
}
``` 

#### External Authentication

External authentication forwards a subrequest with select headers to the external service for
validation. More information can be found in the [NGINX external authentication
documentation][nginx-ext-auth].

```
server {
    listen 10000;

    location / {
       auth_request /auth;
       proxy_pass http://127.0.0.1:8080/;
    }

    # External authentication service
    location = /auth {
        internal;
        proxy_pass              http://127.0.0.1:8081;
        proxy_pass_request_body off;
        proxy_set_header        Content-Length "";
    }
}
```

#### Client Certificates

Clients can be verified and authenticated server-side using TLS along with a certificate provided
by the client. More information about enabling client certificates and TLS can be found in the
[NGINX documentation][nginx-tls].

```
server {
    listen 10000;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    ssl_client_certificate /path/to/ca.pem;
    ssl_verify_client on

    location / {
       proxy_pass http://127.0.0.1:8080/;
    }
}
```

## Using a service mesh ingress controller

If you are using a service mesh such as Istio or linkerd, consider the features that are provided by the
ingress controller for that service mesh. For example, Istio Gateway let's you restrict access to a set of
virtual services that can bind to a server like explained in [this documentation][istio-gateway].

[envoy]: https://www.envoyproxy.io/
[nginx]: https://www.nginx.com/
[istio-gateway]: https://istio.io/docs/reference/config/networking/gateway/
[api-gateway-aws]: https://aws.amazon.com/api-gateway/
[api-gateway-gce]: https://cloud.google.com/endpoints
[api-gateway-azure]: https://azure.microsoft.com/en-us/services/api-management/
[aws-vpc-peering]: https://docs.aws.amazon.com/vpc/latest/peering/what-is-vpc-peering.html
[aws-security-groups]: https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html
[aws-private-api]: https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-private-apis.html
[aws-proxy-integration]: https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-set-up-simple-proxy.html
[envoy-ext-auth]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/security/ext_authz_filter.html
[envoy-jwt]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/security/jwt_authn_filter.html
[envoy-tls]: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/security/ssl
[nginx-ext-auth]: https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-subrequest-authentication/
[nginx-tls]: https://docs.nginx.com/nginx/admin-guide/security-controls/terminating-ssl-http/
