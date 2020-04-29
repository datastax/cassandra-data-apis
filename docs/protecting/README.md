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
Apache Cassandra nodes or within [peered VPC][aws-vpc-peering]. Make sure you only allow ingress from desired
CIDR blocks to the [security group][aws-security-groups]. Also, don't associate public IP addresses to the endpoint.

[Create a private Amazon API Gateway][aws-private-api] that can only be accessed from your virtual private
cloud by assigning the VPC endpoint id(s). Next, [create a child resource, marking it as a
"proxy resource"][aws-proxy-integration], set up integration type as HTTP proxy and enter the endpoint URL.

You can now control access to your API Gateway by managing the subnets and security groups for the VPC endpoint.

## Using a reverse proxy server

You can use a L7 proxy server like [Envoy][envoy] or [NGINX][nginx] to delegate access control and http connection
management, allowing you to place your GraphQL API instance under a private subnet. You can then protect your proxy
instance with http filters, integrating it with your own authorization mechanisms.

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