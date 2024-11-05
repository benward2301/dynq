package dynq.ddb

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

fun createDynamoDbClient(
    endpointUrl: String?,
    profile: String?,
    region: String?,
): DynamoDbClient {
    val regionProvider: AwsRegionProvider
    val credentialsProvider: AwsCredentialsProvider

    if (profile != null) {
        regionProvider = AwsProfileRegionProvider(null, profile)
        credentialsProvider = ProfileCredentialsProvider.create(profile)
    } else {
        regionProvider = DefaultAwsRegionProviderChain()
        credentialsProvider = DefaultCredentialsProvider.create()
    }

    val builder = DynamoDbClient.builder()
        .region(Region.of(region ?: regionProvider.region.id()))
        .credentialsProvider(credentialsProvider)

    if (endpointUrl != null) {
        builder.endpointOverride(URI.create(endpointUrl))
    }

    return builder.build()
}