import sys
from typing import List, Optional, Tuple

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    get_json_object,
    input_file_name,
    lit,
    row_number,
    to_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


# ============================================================
# Glue job arguments
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_database",
        "source_table",
        "redshift_connection_name",
        "redshift_tmp_dir",
        "redshift_database",
        "redshift_schema",
        "target_table",
    ],
)

SOURCE_DATABASE = args["source_database"]
SOURCE_TABLE = args["source_table"]

REDSHIFT_CONNECTION_NAME = args["redshift_connection_name"]
REDSHIFT_TMP_DIR = args["redshift_tmp_dir"].rstrip("/") + "/"
REDSHIFT_DATABASE = args["redshift_database"]
REDSHIFT_SCHEMA = args["redshift_schema"]
TARGET_TABLE = args["target_table"]

STAGING_TABLE = f"{TARGET_TABLE}_staging"

QUALIFIED_TARGET = f"{REDSHIFT_SCHEMA}.{TARGET_TABLE}"
QUALIFIED_STAGING = f"{REDSHIFT_SCHEMA}.{STAGING_TABLE}"


# ============================================================
# Initialise Spark and Glue
# ============================================================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.session.timeZone", "UTC")


# ============================================================
# Read existing preprocessed CTR Glue Catalog table
# ============================================================

source_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="read_existing_ctr_table",
)

source_df = source_dynamic_frame.toDF()

if source_df.rdd.isEmpty():
    print("No CTR records were returned from the source table.")
    job.commit()
    sys.exit(0)

source_df = source_df.withColumn(
    "_source_file",
    input_file_name(),
)

print("Source CTR schema:")
source_df.printSchema()


# ============================================================
# Case-insensitive schema helpers
# ============================================================

def find_field_case_insensitive(
    struct_type: StructType,
    requested_name: str,
) -> Optional[StructField]:
    requested_lower = requested_name.lower()

    for field in struct_type.fields:
        if field.name.lower() == requested_lower:
            return field

    return None


def resolve_path(
    schema: StructType,
    requested_path: str,
) -> Tuple[Optional[str], Optional[DataType]]:
    """
    Resolves a nested field without relying on the source field casing.

    Example:
        Agent.DeviceInfo.PlatformName
    """
    current_type: DataType = schema
    actual_parts: List[str] = []

    for requested_part in requested_path.split("."):
        if not isinstance(current_type, StructType):
            return None, None

        matched_field = find_field_case_insensitive(
            current_type,
            requested_part,
        )

        if matched_field is None:
            return None, None

        actual_parts.append(matched_field.name)
        current_type = matched_field.dataType

    return ".".join(actual_parts), current_type


def value_or_null(
    df: DataFrame,
    requested_path: str,
    spark_type: str,
):
    actual_path, _ = resolve_path(
        df.schema,
        requested_path,
    )

    if actual_path is None:
        return lit(None).cast(spark_type)

    return col(actual_path).cast(spark_type)


def timestamp_or_null(
    df: DataFrame,
    requested_path: str,
):
    actual_path, _ = resolve_path(
        df.schema,
        requested_path,
    )

    if actual_path is None:
        return lit(None).cast("timestamp")

    return to_timestamp(col(actual_path))


def json_text_or_null(
    df: DataFrame,
    requested_path: str,
):
    """
    Returns JSON text for extraction functions such as get_json_object.

    Native structs, arrays and maps are serialised only for extraction.
    String columns that already contain JSON are preserved as strings.
    """
    actual_path, data_type = resolve_path(
        df.schema,
        requested_path,
    )

    if actual_path is None or data_type is None:
        return lit(None).cast("string")

    if isinstance(
        data_type,
        (StructType, ArrayType, MapType),
    ):
        return to_json(col(actual_path))

    return col(actual_path).cast("string")


def complex_or_null(
    df: DataFrame,
    requested_path: str,
):
    """
    Preserves a native Spark StructType, ArrayType or MapType so that the
    Redshift Spark connector writes it directly into a SUPER column.

    A typed null map is returned when the field is absent from the source
    schema. Redshift stores that value as NULL in the SUPER column.
    """
    actual_path, data_type = resolve_path(
        df.schema,
        requested_path,
    )

    if actual_path is None or data_type is None:
        return from_json(
            lit(None).cast("string"),
            MapType(StringType(), StringType()),
        )

    if isinstance(
        data_type,
        (StructType, ArrayType, MapType),
    ):
        return col(actual_path)

    raise TypeError(
        f"{requested_path} is {data_type.simpleString()}, not a native "
        "complex Spark type. Use a field-specific JSON parser."
    )


def string_map_or_null(
    df: DataFrame,
    requested_path: str,
):
    """
    Returns a map<string,string> for dynamic JSON objects such as
    Attributes and Tags.

    The source can already be a Spark map, or it can be a JSON string
    produced by the preprocessing job.
    """
    actual_path, data_type = resolve_path(
        df.schema,
        requested_path,
    )

    target_type = MapType(
        StringType(),
        StringType(),
        valueContainsNull=True,
    )

    if actual_path is None or data_type is None:
        return from_json(
            lit(None).cast("string"),
            target_type,
        )

    if isinstance(data_type, MapType):
        return col(actual_path).cast(target_type)

    if isinstance(data_type, StructType):
        return from_json(
            to_json(col(actual_path)),
            target_type,
        )

    if isinstance(data_type, StringType):
        return from_json(
            col(actual_path),
            target_type,
        )

    raise TypeError(
        f"{requested_path} cannot be converted to map<string,string>; "
        f"source type is {data_type.simpleString()}."
    )


def json_key_value(
    json_expression,
    key_name: str,
):
    """
    Extracts a dynamic JSON key, including keys containing colons.
    """
    escaped_key = key_name.replace("'", "\\'")

    return get_json_object(
        json_expression,
        f"$['{escaped_key}']",
    )


# ============================================================
# Dynamic maps and extraction expressions
# ============================================================

# JSON text is used only for extracting known scalar keys.
attributes_json = json_text_or_null(
    source_df,
    "Attributes",
)

tags_json = json_text_or_null(
    source_df,
    "Tags",
)

segment_attributes_json = json_text_or_null(
    source_df,
    "SegmentAttributes",
)

# Native complex values are written directly to SUPER columns.
attributes_super = string_map_or_null(
    source_df,
    "Attributes",
)

tags_super = string_map_or_null(
    source_df,
    "Tags",
)

segment_attributes_super = complex_or_null(
    source_df,
    "SegmentAttributes",
)

# callData is JSON text stored inside Attributes.
call_data_json = json_key_value(
    attributes_json,
    "callData",
)

call_data_super = from_json(
    call_data_json,
    MapType(
        StringType(),
        StringType(),
        valueContainsNull=True,
    ),
)


# ============================================================
# Create flattened CTR DataFrame
# ============================================================

flattened_df = source_df.select(
    # --------------------------------------------------------
    # Main CTR fields
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "AWSAccountId",
        "string",
    ).alias("aws_account_id"),

    value_or_null(
        source_df,
        "AWSContactTraceRecordFormatVersion",
        "string",
    ).alias("aws_ctr_format_version"),

    value_or_null(
        source_df,
        "ContactId",
        "string",
    ).alias("contact_id"),

    value_or_null(
        source_df,
        "ContactAssociationId",
        "string",
    ).alias("contact_association_id"),

    value_or_null(
        source_df,
        "InitialContactId",
        "string",
    ).alias("initial_contact_id"),

    value_or_null(
        source_df,
        "PreviousContactId",
        "string",
    ).alias("previous_contact_id"),

    value_or_null(
        source_df,
        "NextContactId",
        "string",
    ).alias("next_contact_id"),

    value_or_null(
        source_df,
        "InstanceARN",
        "string",
    ).alias("instance_arn"),

    value_or_null(
        source_df,
        "Channel",
        "string",
    ).alias("channel"),

    value_or_null(
        source_df,
        "InitiationMethod",
        "string",
    ).alias("initiation_method"),

    value_or_null(
        source_df,
        "DisconnectReason",
        "string",
    ).alias("disconnect_reason"),

    value_or_null(
        source_df,
        "AnsweringMachineDetectionStatus",
        "string",
    ).alias("answering_machine_detection_status"),

    value_or_null(
        source_df,
        "AgentConnectionAttempts",
        "long",
    ).alias("agent_connection_attempts"),

    timestamp_or_null(
        source_df,
        "InitiationTimestamp",
    ).alias("initiation_timestamp"),

    timestamp_or_null(
        source_df,
        "ConnectedToSystemTimestamp",
    ).alias("connected_to_system_timestamp"),

    timestamp_or_null(
        source_df,
        "DisconnectTimestamp",
    ).alias("disconnect_timestamp"),

    timestamp_or_null(
        source_df,
        "ScheduledTimestamp",
    ).alias("scheduled_timestamp"),

    timestamp_or_null(
        source_df,
        "TransferCompletedTimestamp",
    ).alias("transfer_completed_timestamp"),

    timestamp_or_null(
        source_df,
        "LastUpdateTimestamp",
    ).alias("last_update_timestamp"),

    # --------------------------------------------------------
    # Agent
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "Agent.ARN",
        "string",
    ).alias("agent_arn"),

    value_or_null(
        source_df,
        "Agent.ActiveRegion",
        "string",
    ).alias("agent_active_region"),

    value_or_null(
        source_df,
        "Agent.Username",
        "string",
    ).alias("agent_username"),

    value_or_null(
        source_df,
        "Agent.AfterContactWorkDuration",
        "long",
    ).alias("agent_after_contact_work_duration"),

    timestamp_or_null(
        source_df,
        "Agent.AfterContactWorkStartTimestamp",
    ).alias("agent_after_contact_work_start_ts"),

    timestamp_or_null(
        source_df,
        "Agent.AfterContactWorkEndTimestamp",
    ).alias("agent_after_contact_work_end_ts"),

    value_or_null(
        source_df,
        "Agent.AgentInitiatedHoldDuration",
        "long",
    ).alias("agent_initiated_hold_duration"),

    value_or_null(
        source_df,
        "Agent.AgentInteractionDuration",
        "long",
    ).alias("agent_interaction_duration"),

    timestamp_or_null(
        source_df,
        "Agent.ConnectedToAgentTimestamp",
    ).alias("agent_connected_timestamp"),

    value_or_null(
        source_df,
        "Agent.CustomerHoldDuration",
        "long",
    ).alias("agent_customer_hold_duration"),

    value_or_null(
        source_df,
        "Agent.LongestHoldDuration",
        "long",
    ).alias("agent_longest_hold_duration"),

    value_or_null(
        source_df,
        "Agent.NumberOfHolds",
        "long",
    ).alias("agent_number_of_holds"),

    value_or_null(
        source_df,
        "Agent.VoiceEnhancementMode",
        "string",
    ).alias("agent_voice_enhancement_mode"),

    value_or_null(
        source_df,
        "Agent.DeviceInfo.OperatingSystem",
        "string",
    ).alias("agent_device_operating_system"),

    value_or_null(
        source_df,
        "Agent.DeviceInfo.PlatformName",
        "string",
    ).alias("agent_device_platform_name"),

    value_or_null(
        source_df,
        "Agent.DeviceInfo.PlatformVersion",
        "string",
    ).alias("agent_device_platform_version"),

    value_or_null(
        source_df,
        "Agent.RoutingProfile.ARN",
        "string",
    ).alias("agent_routing_profile_arn"),

    value_or_null(
        source_df,
        "Agent.RoutingProfile.Name",
        "string",
    ).alias("agent_routing_profile_name"),

    complex_or_null(
        source_df,
        "Agent.HierarchyGroups",
    ).alias("agent_hierarchy_groups"),

    complex_or_null(
        source_df,
        "Agent.StateTransitions",
    ).alias("agent_state_transitions"),

    # --------------------------------------------------------
    # Campaign
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "Campaign.CampaignId",
        "string",
    ).alias("campaign_id"),

    # --------------------------------------------------------
    # Endpoints
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "CustomerEndpoint.Address",
        "string",
    ).alias("customer_endpoint_address"),

    value_or_null(
        source_df,
        "CustomerEndpoint.Type",
        "string",
    ).alias("customer_endpoint_type"),

    value_or_null(
        source_df,
        "SystemEndpoint.Address",
        "string",
    ).alias("system_endpoint_address"),

    value_or_null(
        source_df,
        "SystemEndpoint.Type",
        "string",
    ).alias("system_endpoint_type"),

    value_or_null(
        source_df,
        "TransferredToEndpoint.Address",
        "string",
    ).alias("transferred_to_endpoint_address"),

    value_or_null(
        source_df,
        "TransferredToEndpoint.Type",
        "string",
    ).alias("transferred_to_endpoint_type"),

    # --------------------------------------------------------
    # Queue
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "Queue.ARN",
        "string",
    ).alias("queue_arn"),

    value_or_null(
        source_df,
        "Queue.Name",
        "string",
    ).alias("queue_name"),

    value_or_null(
        source_df,
        "Queue.Duration",
        "long",
    ).alias("queue_duration"),

    timestamp_or_null(
        source_df,
        "Queue.EnqueueTimestamp",
    ).alias("queue_enqueue_timestamp"),

    timestamp_or_null(
        source_df,
        "Queue.DequeueTimestamp",
    ).alias("queue_dequeue_timestamp"),

    # --------------------------------------------------------
    # Singular recording
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "Recording.DeletionReason",
        "string",
    ).alias("recording_deletion_reason"),

    value_or_null(
        source_df,
        "Recording.Location",
        "string",
    ).alias("recording_location"),

    value_or_null(
        source_df,
        "Recording.Status",
        "string",
    ).alias("recording_status"),

    value_or_null(
        source_df,
        "Recording.Type",
        "string",
    ).alias("recording_type"),

    # --------------------------------------------------------
    # Contact Lens
    # --------------------------------------------------------
    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics."
            "Configuration.Enabled"
        ),
        "boolean",
    ).alias("contact_lens_enabled"),

    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics."
            "Configuration.LanguageLocale"
        ),
        "string",
    ).alias("contact_lens_language_locale"),

    complex_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "ChannelConfiguration.AnalyticsModes"
        ),
    ).alias("contact_lens_analytics_modes"),

    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "RedactionConfiguration.Behavior"
        ),
        "string",
    ).alias("contact_lens_redaction_behavior"),

    complex_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "RedactionConfiguration.Entities"
        ),
    ).alias("contact_lens_redaction_entities"),

    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "RedactionConfiguration.MaskMode"
        ),
        "string",
    ).alias("contact_lens_redaction_mask_mode"),

    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "RedactionConfiguration.Policy"
        ),
        "string",
    ).alias("contact_lens_redaction_policy"),

    value_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "SentimentConfiguration.Behavior"
        ),
        "string",
    ).alias("contact_lens_sentiment_behavior"),

    complex_or_null(
        source_df,
        (
            "ContactLens.ConversationalAnalytics.Configuration."
            "SummaryConfiguration"
        ),
    ).alias("contact_lens_summary_configuration"),

    # --------------------------------------------------------
    # Quality metrics
    # --------------------------------------------------------
    value_or_null(
        source_df,
        "QualityMetrics.Agent.Audio.QualityScore",
        "double",
    ).alias("quality_agent_audio_score"),

    complex_or_null(
        source_df,
        "QualityMetrics.Agent.Audio.PotentialQualityIssues",
    ).alias("quality_agent_audio_issues"),

    value_or_null(
        source_df,
        "QualityMetrics.Customer.Audio.QualityScore",
        "double",
    ).alias("quality_customer_audio_score"),

    complex_or_null(
        source_df,
        "QualityMetrics.Customer.Audio.PotentialQualityIssues",
    ).alias("quality_customer_audio_issues"),

    # --------------------------------------------------------
    # Known custom Attributes
    # --------------------------------------------------------
    json_key_value(
        attributes_json,
        "AnalyticsProvider",
    ).alias("attribute_analytics_provider"),

    json_key_value(
        attributes_json,
        "CallerCifKey",
    ).alias("attribute_caller_cif_key"),

    json_key_value(
        attributes_json,
        "CallerIdNumber",
    ).alias("attribute_caller_id_number"),

    json_key_value(
        attributes_json,
        "CallerIdType",
    ).alias("attribute_caller_id_type"),

    json_key_value(
        attributes_json,
        "CallerName",
    ).alias("attribute_caller_name"),

    json_key_value(
        attributes_json,
        "CallerPhoneNumber",
    ).alias("attribute_caller_phone_number"),

    json_key_value(
        attributes_json,
        "ContactFlowId",
    ).alias("attribute_contact_flow_id"),

    json_key_value(
        attributes_json,
        "ContextManagerSessionId",
    ).alias("attribute_context_manager_session_id"),

    json_key_value(
        attributes_json,
        "CustomerNumber",
    ).alias("attribute_customer_number"),

    json_key_value(
        attributes_json,
        "IsAnalyticsEnabled",
    ).alias("attribute_is_analytics_enabled"),

    json_key_value(
        attributes_json,
        "IsAuthenticated",
    ).alias("attribute_is_authenticated"),

    json_key_value(
        attributes_json,
        "IsChatAnalyticsEnabled",
    ).alias("attribute_is_chat_analytics_enabled"),

    json_key_value(
        attributes_json,
        "IsIdentified",
    ).alias("attribute_is_identified"),

    json_key_value(
        attributes_json,
        "IsScreenRecordingEnabled",
    ).alias("attribute_is_screen_recording_enabled"),

    json_key_value(
        attributes_json,
        "IsSpeechAnalyticsEnabled",
    ).alias("attribute_is_speech_analytics_enabled"),

    json_key_value(
        attributes_json,
        "IsSurveyEnabled",
    ).alias("attribute_is_survey_enabled"),

    json_key_value(
        attributes_json,
        "SurveyId",
    ).alias("attribute_survey_id"),

    json_key_value(
        attributes_json,
        "accountNo",
    ).alias("attribute_account_no"),

    json_key_value(
        attributes_json,
        "allLinkedAccounts",
    ).alias("attribute_all_linked_accounts"),

    json_key_value(
        attributes_json,
        "chosenAccountObject",
    ).alias("attribute_chosen_account_object"),

    json_key_value(
        attributes_json,
        "cifKey",
    ).alias("attribute_cif_key"),

    json_key_value(
        attributes_json,
        "clientGroup",
    ).alias("attribute_client_group"),

    json_key_value(
        attributes_json,
        "contactFlowName",
    ).alias("attribute_contact_flow_name"),

    json_key_value(
        attributes_json,
        "customerId",
    ).alias("attribute_customer_id"),

    json_key_value(
        attributes_json,
        "evalReturnCode",
    ).alias("attribute_eval_return_code"),

    json_key_value(
        attributes_json,
        "fromTelephoneBanking",
    ).alias("attribute_from_telephone_banking"),

    json_key_value(
        attributes_json,
        "homeLanguageCode",
    ).alias("attribute_home_language_code"),

    json_key_value(
        attributes_json,
        "idNumber",
    ).alias("attribute_id_number"),

    json_key_value(
        attributes_json,
        "pinType",
    ).alias("attribute_pin_type"),

    json_key_value(
        attributes_json,
        "registrationStatus",
    ).alias("attribute_registration_status"),

    json_key_value(
        attributes_json,
        "sbuSegment",
    ).alias("attribute_sbu_segment"),

    json_key_value(
        attributes_json,
        "sendTo",
    ).alias("attribute_send_to"),

    json_key_value(
        attributes_json,
        "statusFICA",
    ).alias("attribute_status_fica"),

    json_key_value(
        attributes_json,
        "FicComplete",
    ).alias("attribute_fic_complete"),

    attributes_super.alias("attributes"),

    # --------------------------------------------------------
    # Fields inside Attributes.callData
    # --------------------------------------------------------
    json_key_value(
        call_data_json,
        "defaultANI",
    ).alias("call_data_default_ani"),

    json_key_value(
        call_data_json,
        "cifKey",
    ).alias("call_data_cif_key"),

    json_key_value(
        call_data_json,
        "idNumber",
    ).alias("call_data_id_number"),

    json_key_value(
        call_data_json,
        "connectionStatus",
    ).alias("call_data_connection_status"),

    json_key_value(
        call_data_json,
        "sourceNo",
    ).alias("call_data_source_no"),

    json_key_value(
        call_data_json,
        "destinationNo",
    ).alias("call_data_destination_no"),

    json_key_value(
        call_data_json,
        "queueName",
    ).alias("call_data_queue_name"),

    json_key_value(
        call_data_json,
        "connectionId",
    ).alias("call_data_connection_id"),

    json_key_value(
        call_data_json,
        "contextId",
    ).alias("call_data_context_id"),

    json_key_value(
        call_data_json,
        "isIdentified",
    ).alias("call_data_is_identified"),

    json_key_value(
        call_data_json,
        "isAuthenticated",
    ).alias("call_data_is_authenticated"),

    call_data_super.alias("call_data"),

    # --------------------------------------------------------
    # Tags
    # --------------------------------------------------------
    json_key_value(
        tags_json,
        "BillingCostCenter",
    ).alias("tag_billing_cost_center"),

    json_key_value(
        tags_json,
        "BillingDepartment",
    ).alias("tag_billing_department"),

    json_key_value(
        tags_json,
        "BillingDivision",
    ).alias("tag_billing_division"),

    json_key_value(
        tags_json,
        "SpeechAnalytics",
    ).alias("tag_speech_analytics"),

    json_key_value(
        tags_json,
        "aws:connect:instanceId",
    ).alias("tag_aws_connect_instance_id"),

    json_key_value(
        tags_json,
        "aws:connect:systemEndpoint",
    ).alias("tag_aws_connect_system_endpoint"),

    tags_super.alias("tags"),

    # --------------------------------------------------------
    # Segment attributes
    # --------------------------------------------------------
    get_json_object(
        segment_attributes_json,
        "$['connect:Subtype'].ValueString",
    ).alias("segment_connect_subtype"),

    get_json_object(
        segment_attributes_json,
        (
            "$['connect:Purpose'].ValueMap.analytics."
            "ValueList[0].ValueString"
        ),
    ).alias("segment_purpose_analytics_reference"),

    get_json_object(
        segment_attributes_json,
        (
            "$['connect:Purpose'].ValueMap."
            "contact-attributes-search.ValueList[0].ValueString"
        ),
    ).alias("segment_purpose_contact_search_reference"),

    segment_attributes_super.alias("segment_attributes"),

    # --------------------------------------------------------
    # Other complex structures
    # --------------------------------------------------------
    complex_or_null(
        source_df,
        "ContactDetails",
    ).alias("contact_details"),

    complex_or_null(
        source_df,
        "CustomerVoiceActivity",
    ).alias("customer_voice_activity"),

    complex_or_null(
        source_df,
        "TaskTemplateInfo",
    ).alias("task_template_info"),

    complex_or_null(
        source_df,
        "VoiceIdResult",
    ).alias("voice_id_result"),

    complex_or_null(
        source_df,
        "Customer",
    ).alias("customer"),

    complex_or_null(
        source_df,
        "ChatMetrics",
    ).alias("chat_metrics"),

    complex_or_null(
        source_df,
        "ContactRoutingData",
    ).alias("contact_routing_data"),

    complex_or_null(
        source_df,
        "ExternalThirdParty",
    ).alias("external_third_party"),

    complex_or_null(
        source_df,
        "DisconnectDetails",
    ).alias("disconnect_details"),

    # --------------------------------------------------------
    # Arrays
    # --------------------------------------------------------
    complex_or_null(
        source_df,
        "MediaStreams",
    ).alias("media_streams"),

    complex_or_null(
        source_df,
        "Recordings",
    ).alias("recordings"),

    complex_or_null(
        source_df,
        "References",
    ).alias("references_data"),

    # --------------------------------------------------------
    # Audit fields
    # --------------------------------------------------------
    col("_source_file").cast("string").alias("source_file"),
    current_timestamp().alias("etl_loaded_timestamp"),
)


# ============================================================
# Remove invalid records
# ============================================================

flattened_df = flattened_df.filter(
    col("contact_id").isNotNull()
)


# ============================================================
# Deduplicate current batch
#
# Keep only the newest version of each ContactId.
# ============================================================

latest_contact_window = (
    Window
    .partitionBy("contact_id")
    .orderBy(
        col("last_update_timestamp").desc_nulls_last(),
        col("etl_loaded_timestamp").desc(),
    )
)

flattened_df = (
    flattened_df
    .withColumn(
        "_row_number",
        row_number().over(latest_contact_window),
    )
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)


row_count = flattened_df.count()

print(f"Flattened CTR rows for this run: {row_count}")
print("Flattened output schema:")
flattened_df.printSchema()

if row_count == 0:
    print("No valid CTR rows remain after filtering.")
    job.commit()
    sys.exit(0)


# ============================================================
# Redshift column lists
#
# Staging and target have identical schemas, including SUPER columns.
# ============================================================

all_columns = flattened_df.columns

target_column_sql = ", ".join(
    f'"{column_name}"'
    for column_name in all_columns
)

source_column_sql = ", ".join(
    f'source."{column_name}"'
    for column_name in all_columns
)


# ============================================================
# Redshift preactions
# ============================================================

preactions = f"""
TRUNCATE TABLE {QUALIFIED_STAGING};
"""


# ============================================================
# Redshift upsert
#
# 1. Delete the existing target row only when the incoming row
#    is newer or equally recent.
# 2. Insert new or updated rows.
# 3. Ignore older incoming versions.
# ============================================================

postactions = f"""
BEGIN;

DELETE FROM {QUALIFIED_TARGET}
USING {QUALIFIED_STAGING}
WHERE {QUALIFIED_TARGET}.contact_id =
      {QUALIFIED_STAGING}.contact_id
  AND (
        {QUALIFIED_TARGET}.last_update_timestamp IS NULL
        OR {QUALIFIED_STAGING}.last_update_timestamp IS NULL
        OR {QUALIFIED_STAGING}.last_update_timestamp >=
           {QUALIFIED_TARGET}.last_update_timestamp
      );

INSERT INTO {QUALIFIED_TARGET}
(
    {target_column_sql}
)
SELECT
    {source_column_sql}
FROM {QUALIFIED_STAGING} AS source
WHERE NOT EXISTS
(
    SELECT 1
    FROM {QUALIFIED_TARGET} AS target
    WHERE target.contact_id = source.contact_id
);

TRUNCATE TABLE {QUALIFIED_STAGING};

END;
"""


# ============================================================
# Write to Redshift staging and run the upsert
# ============================================================

output_dynamic_frame = DynamicFrame.fromDF(
    flattened_df,
    glue_context,
    "flattened_ctr_output",
)

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=output_dynamic_frame,
    catalog_connection=REDSHIFT_CONNECTION_NAME,
    connection_options={
        "dbtable": QUALIFIED_STAGING,
        "database": REDSHIFT_DATABASE,
        "preactions": preactions,
        "postactions": postactions,
        "tempformat": "PARQUET",
    },
    redshift_tmp_dir=REDSHIFT_TMP_DIR,
    transformation_ctx="write_ctr_flattened_to_redshift",
)

job.commit()
