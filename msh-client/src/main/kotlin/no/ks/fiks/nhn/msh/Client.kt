package no.ks.fiks.nhn.msh

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import feign.Feign
import feign.jackson.JacksonDecoder
import feign.jackson.JacksonEncoder
import no.ks.fiks.hdir.FeilmeldingForApplikasjonskvittering
import no.ks.fiks.hdir.StatusForMottakAvMelding
import no.ks.fiks.helseid.dpop.Endpoint
import no.ks.fiks.helseid.dpop.HttpMethod
import no.ks.fiks.helseid.http.HttpRequestHelper
import no.ks.fiks.nhn.ar.AdresseregisteretClient
import no.ks.fiks.nhn.edi.BusinessDocumentDeserializer
import no.ks.fiks.nhn.edi.BusinessDocumentSerializer
import no.ks.fiks.nhn.flr.FastlegeregisteretClient
import no.nhn.msh.v2.api.MessagesControllerApi
import no.nhn.msh.v2.model.AppRecError
import no.nhn.msh.v2.model.AppRecStatus
import no.nhn.msh.v2.model.PostAppRecRequest
import no.nhn.msh.v2.model.PostMessageRequest
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import no.ks.fiks.helseid.Configuration as HelseIdConfiguration
import no.ks.fiks.nhn.ar.Credentials as ArCredentials
import no.ks.fiks.nhn.flr.Credentials as FlrCredentials
import no.nhn.msh.v2.model.Message as NhnMessage

private const val API_VERSION = "2"

private const val CONTENT_TYPE = "application/xml"
private const val CONTENT_TRANSFER_ENCODING = "base64"

// Meldingstjener, MSH (Message Service Handler)
class Client(
    configuration: Configuration,
) {

    private val baseUrl = configuration.environments.mshEnvironment.url

    private val mapper = ObjectMapper()
        .registerModule(JavaTimeModule())
        .registerModule(SimpleModule().apply {
            addDeserializer(OffsetDateTime::class.java, object : JsonDeserializer<OffsetDateTime>() { // API returns ISO 8601 dates without offset that can't be parsed by the default deserializer
                override fun deserialize(parser: JsonParser, context: DeserializationContext): OffsetDateTime {
                    val local = LocalDateTime.parse(parser.text)
                    return OffsetDateTime.of(local, ZoneOffset.UTC)
                }
            })
        })

    private val flrClient = FastlegeregisteretClient(
        environment = configuration.environments.fastlegeregisteretEnvironment,
        credentials = configuration.fastlegeregisteret.let {
            FlrCredentials(
                username = it.username,
                password = it.password,
            )
        },
    )
    private val arClient = AdresseregisteretClient(
        environment = configuration.environments.adresseregisteretEnvironment,
        credentials = configuration.adresseregisteret.let {
            ArCredentials(
                username = it.username,
                password = it.password,
            )
        },
    )
    private val receiverBuilder = GpForPersonReceiverBuilder(flrClient, arClient)

    private val httpHelper = HttpRequestHelper(
        HelseIdConfiguration(
            environment = configuration.environments.helseIdEnvironment,
            clientId = configuration.helseId.clientId,
            jwk = configuration.helseId.jwk,
        )
    )
    private val meldingstjenerEnvironment = configuration.environments.mshEnvironment
    private val sourceSystem = configuration.sourceSystem

    fun sendMessageToGPForPerson(businessDocument: GPForPersonOutgoingBusinessDocument) {
        sendMessage(
            OutgoingBusinessDocument(
                id = businessDocument.id,
                sender = businessDocument.sender,
                receiver = receiverBuilder.buildGpForPersonReceiver(businessDocument.person),
                message = businessDocument.message,
                vedlegg = businessDocument.vedlegg,
                version = businessDocument.version,
            )
        )
    }

    fun sendMessage(businessDocument: OutgoingBusinessDocument,enhet:String? = null) {
        buildClient(buildPostMessagesEndpoint(),enhet)
            .postMessage(
                API_VERSION, sourceSystem, PostMessageRequest()
                    .contentType(CONTENT_TYPE)
                    .contentTransferEncoding(CONTENT_TRANSFER_ENCODING)
                    .businessDocument(Base64.getEncoder().encodeToString(BusinessDocumentSerializer.serializeNhnMessage(businessDocument).toByteArray()))
            )
    }

    fun getMessages(receiverHerId: Int,ehet: String?=null): List<Message> {
        return buildClient(buildGetMessagesEndpoint(),ehet)
            .getMessages(
                API_VERSION, sourceSystem, MessagesControllerApi.GetMessagesQueryParams()
                    .receiverHerIds(setOf(receiverHerId))
            )
            .map { it.toMessageInfo() }

    }

    fun getMessagesWithMetadata(receiverHerId: Int): List<MessageWithMetadata> {
        return buildClient(buildGetMessagesEndpoint())
            .getMessages(
                API_VERSION, sourceSystem, MessagesControllerApi.GetMessagesQueryParams()
                    .receiverHerIds(setOf(receiverHerId))
                    .includeMetadata(true)
            )
            .map { it.toMessageInfoWithMetadata() }

    }

    fun getMessage(id: UUID,enhet: String?=null): MessageWithMetadata {
        return buildClient(buildGetMessageByIdEndpoint(id),enhet)
            .getMessage(id, API_VERSION, sourceSystem)
            .toMessageInfoWithMetadata()
    }

    fun getBusinessDocument(id: UUID,enhet:String?=null): IncomingBusinessDocument {
        return buildClient(buildGetBusinessDocumentEndpoint(id),enhet)
            .getBusinessDocument(id, API_VERSION, sourceSystem)
            .let {
                BusinessDocumentDeserializer.deserializeMsgHead(String(Base64.getDecoder().decode(it.businessDocument)))
            }
    }

    fun getApplicationReceipt(id: UUID,enhet: String?=null): IncomingApplicationReceipt {
        return buildClient(buildGetBusinessDocumentEndpoint(id),enhet)
            .getBusinessDocument(id, API_VERSION, sourceSystem)
            .let {
                BusinessDocumentDeserializer.deserializeAppRec(String(Base64.getDecoder().decode(it.businessDocument)))
            }
    }

    fun sendApplicationReceipt(receipt: OutgoingApplicationReceipt,enhet: String?=null) {
        if (receipt.status == StatusForMottakAvMelding.OK && !receipt.errors.isNullOrEmpty()) throw IllegalArgumentException("Error messages are not allowed when status is OK")
        if (receipt.status != StatusForMottakAvMelding.OK && receipt.errors.isNullOrEmpty()) throw IllegalArgumentException("Must provide at least one error message if status is not OK")

        buildClient(buildPostAppRecEndpoint(receipt.acknowledgedId, receipt.senderHerId),enhet)
            .postAppRec(
                receipt.acknowledgedId, receipt.senderHerId, API_VERSION, sourceSystem, PostAppRecRequest()
                    .appRecStatus(receipt.status.toAppRecStatus())
                    .appRecErrorList(receipt.errors?.toAppRecErrors())
            )
    }

    private fun StatusForMottakAvMelding.toAppRecStatus() = when (this) {
        StatusForMottakAvMelding.OK -> AppRecStatus.OK
        StatusForMottakAvMelding.OK_FEIL_I_DELMELDING -> AppRecStatus.OK_ERROR_IN_MESSAGE_PART
        StatusForMottakAvMelding.AVVIST -> AppRecStatus.REJECTED
    }

    private fun List<ApplicationReceiptError>.toAppRecErrors() = map { it.toAppRecError() }

    private fun ApplicationReceiptError.toAppRecError(): AppRecError {
        if (type == FeilmeldingForApplikasjonskvittering.UKJENT) throw IllegalArgumentException("Ukjent is not a valid error type for an outgoing application receipt")
        return AppRecError()
            .errorCode(type.verdi)
            .details(details)
    }

    fun markMessageRead(id: UUID, receiverHerId: Int,enhet:String?=null) {
        buildClient(buildPutMessageReadEndpoint(id, receiverHerId),enhet)
            .markMessageAsRead(id, receiverHerId, API_VERSION, sourceSystem)
    }

    private fun NhnMessage.toMessageInfo() = Message(
        id = id,
        receiverHerId = receiverHerId,
    )

    private fun NhnMessage.toMessageInfoWithMetadata() = MessageWithMetadata(
        id = id,
        contentType = contentType,
        receiverHerId = receiverHerId,
        senderHerId = senderHerId,
        businessDocumentId = businessDocumentId,
        businessDocumentDate = businessDocumentGenDate,
        isAppRec = isAppRec,
    )


    private fun buildClient(endpoint: Endpoint, enhet:String?=null) = Feign.builder()
        .encoder(JacksonEncoder(mapper))
        .decoder(JacksonDecoder(mapper))
        .requestInterceptor {
            httpHelper.addDpopAuthorizationHeader(endpoint,enhet) { name, value ->
                it.header(name, value)
            }
        }
        .target(MessagesControllerApi::class.java, meldingstjenerEnvironment.url)

    private fun buildGetMessagesEndpoint() = Endpoint(HttpMethod.GET, "$baseUrl/Messages")
    private fun buildPostMessagesEndpoint() = Endpoint(HttpMethod.POST, "$baseUrl/Messages")
    private fun buildGetMessageByIdEndpoint(id: UUID) = Endpoint(HttpMethod.GET, "$baseUrl/Messages/$id")
    private fun buildGetBusinessDocumentEndpoint(id: UUID) = Endpoint(HttpMethod.GET, "$baseUrl/Messages/$id/business-document")
    private fun buildPostAppRecEndpoint(id: UUID, senderHerId: Int) = Endpoint(HttpMethod.POST, "$baseUrl/Messages/$id/apprec/$senderHerId")
    private fun buildPutMessageReadEndpoint(id: UUID, receiverHerId: Int) = Endpoint(HttpMethod.PUT, "$baseUrl/Messages/$id/read/$receiverHerId")

}