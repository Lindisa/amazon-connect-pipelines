SELECT
    column_name,
    COUNT(*) AS empty_value_count
FROM (
    SELECT
        -- paste the complete string-column list here
        aws_account_id,
        aws_contact_trace_record_format_version,
        contact_id,
        contact_association_id,
        initial_contact_id,
        previous_contact_id,
        next_contact_id,
        related_contact_id,
        instance_arn,
        channel,
        initiation_method,
        disconnect_reason,
        answering_machine_detection_status,

        -- Agent, campaign, endpoints, queue, recording,
        -- Contact Lens, Attributes, callData, Tags,
        -- Segment fields and source_file from the previous list
        source_file
    FROM public.ctr_flattened
) string_columns
UNPIVOT (
    column_value FOR column_name IN (
        -- paste the exact same complete string-column list here
        aws_account_id,
        aws_contact_trace_record_format_version,
        contact_id,
        contact_association_id,
        initial_contact_id,
        previous_contact_id,
        next_contact_id,
        related_contact_id,
        instance_arn,
        channel,
        initiation_method,
        disconnect_reason,
        answering_machine_detection_status,

        -- same remaining columns
        source_file
    )
)
WHERE TRIM(column_value) = ''
GROUP BY column_name
ORDER BY empty_value_count DESC, column_name;






SELECT
    SUM(CASE WHEN attribute_analytics_provider IS NOT NULL
                  AND TRIM(attribute_analytics_provider) = '' THEN 1 ELSE 0 END)
        AS attribute_analytics_provider_empty,

    SUM(CASE WHEN attribute_caller_cif_key IS NOT NULL
                  AND TRIM(attribute_caller_cif_key) = '' THEN 1 ELSE 0 END)
        AS attribute_caller_cif_key_empty,

    SUM(CASE WHEN attribute_caller_id_number IS NOT NULL
                  AND TRIM(attribute_caller_id_number) = '' THEN 1 ELSE 0 END)
        AS attribute_caller_id_number_empty,

    SUM(CASE WHEN attribute_caller_id_type IS NOT NULL
                  AND TRIM(attribute_caller_id_type) = '' THEN 1 ELSE 0 END)
        AS attribute_caller_id_type_empty,

    SUM(CASE WHEN attribute_caller_name IS NOT NULL
                  AND TRIM(attribute_caller_name) = '' THEN 1 ELSE 0 END)
        AS attribute_caller_name_empty,

    SUM(CASE WHEN attribute_caller_phone_number IS NOT NULL
                  AND TRIM(attribute_caller_phone_number) = '' THEN 1 ELSE 0 END)
        AS attribute_caller_phone_number_empty,

    SUM(CASE WHEN attribute_contact_flow_id IS NOT NULL
                  AND TRIM(attribute_contact_flow_id) = '' THEN 1 ELSE 0 END)
        AS attribute_contact_flow_id_empty,

    SUM(CASE WHEN attribute_context_manager_session_id IS NOT NULL
                  AND TRIM(attribute_context_manager_session_id) = '' THEN 1 ELSE 0 END)
        AS attribute_context_manager_session_id_empty,

    SUM(CASE WHEN attribute_customer_number IS NOT NULL
                  AND TRIM(attribute_customer_number) = '' THEN 1 ELSE 0 END)
        AS attribute_customer_number_empty,

    SUM(CASE WHEN attribute_is_analytics_enabled IS NOT NULL
                  AND TRIM(attribute_is_analytics_enabled) = '' THEN 1 ELSE 0 END)
        AS attribute_is_analytics_enabled_empty,

    SUM(CASE WHEN attribute_is_authenticated IS NOT NULL
                  AND TRIM(attribute_is_authenticated) = '' THEN 1 ELSE 0 END)
        AS attribute_is_authenticated_empty,

    SUM(CASE WHEN attribute_is_chat_analytics_enabled IS NOT NULL
                  AND TRIM(attribute_is_chat_analytics_enabled) = '' THEN 1 ELSE 0 END)
        AS attribute_is_chat_analytics_enabled_empty,

    SUM(CASE WHEN attribute_is_identified IS NOT NULL
                  AND TRIM(attribute_is_identified) = '' THEN 1 ELSE 0 END)
        AS attribute_is_identified_empty,

    SUM(CASE WHEN attribute_is_screen_recording_enabled IS NOT NULL
                  AND TRIM(attribute_is_screen_recording_enabled) = '' THEN 1 ELSE 0 END)
        AS attribute_is_screen_recording_enabled_empty,

    SUM(CASE WHEN attribute_is_speech_analytics_enabled IS NOT NULL
                  AND TRIM(attribute_is_speech_analytics_enabled) = '' THEN 1 ELSE 0 END)
        AS attribute_is_speech_analytics_enabled_empty,

    SUM(CASE WHEN attribute_is_survey_enabled IS NOT NULL
                  AND TRIM(attribute_is_survey_enabled) = '' THEN 1 ELSE 0 END)
        AS attribute_is_survey_enabled_empty,

    SUM(CASE WHEN attribute_survey_id IS NOT NULL
                  AND TRIM(attribute_survey_id) = '' THEN 1 ELSE 0 END)
        AS attribute_survey_id_empty,

    SUM(CASE WHEN attribute_account_no IS NOT NULL
                  AND TRIM(attribute_account_no) = '' THEN 1 ELSE 0 END)
        AS attribute_account_no_empty,

    SUM(CASE WHEN attribute_all_linked_accounts IS NOT NULL
                  AND TRIM(attribute_all_linked_accounts) = '' THEN 1 ELSE 0 END)
        AS attribute_all_linked_accounts_empty,

    SUM(CASE WHEN attribute_chosen_account_object IS NOT NULL
                  AND TRIM(attribute_chosen_account_object) = '' THEN 1 ELSE 0 END)
        AS attribute_chosen_account_object_empty,

    SUM(CASE WHEN attribute_cif_key IS NOT NULL
                  AND TRIM(attribute_cif_key) = '' THEN 1 ELSE 0 END)
        AS attribute_cif_key_empty,

    SUM(CASE WHEN attribute_client_group IS NOT NULL
                  AND TRIM(attribute_client_group) = '' THEN 1 ELSE 0 END)
        AS attribute_client_group_empty,

    SUM(CASE WHEN attribute_contact_flow_name IS NOT NULL
                  AND TRIM(attribute_contact_flow_name) = '' THEN 1 ELSE 0 END)
        AS attribute_contact_flow_name_empty,

    SUM(CASE WHEN attribute_customer_id IS NOT NULL
                  AND TRIM(attribute_customer_id) = '' THEN 1 ELSE 0 END)
        AS attribute_customer_id_empty,

    SUM(CASE WHEN attribute_eval_return_code IS NOT NULL
                  AND TRIM(attribute_eval_return_code) = '' THEN 1 ELSE 0 END)
        AS attribute_eval_return_code_empty,

    SUM(CASE WHEN attribute_from_telephone_banking IS NOT NULL
                  AND TRIM(attribute_from_telephone_banking) = '' THEN 1 ELSE 0 END)
        AS attribute_from_telephone_banking_empty,

    SUM(CASE WHEN attribute_home_language_code IS NOT NULL
                  AND TRIM(attribute_home_language_code) = '' THEN 1 ELSE 0 END)
        AS attribute_home_language_code_empty,

    SUM(CASE WHEN attribute_id_number IS NOT NULL
                  AND TRIM(attribute_id_number) = '' THEN 1 ELSE 0 END)
        AS attribute_id_number_empty,

    SUM(CASE WHEN attribute_pin_type IS NOT NULL
                  AND TRIM(attribute_pin_type) = '' THEN 1 ELSE 0 END)
        AS attribute_pin_type_empty,

    SUM(CASE WHEN attribute_registration_status IS NOT NULL
                  AND TRIM(attribute_registration_status) = '' THEN 1 ELSE 0 END)
        AS attribute_registration_status_empty,

    SUM(CASE WHEN attribute_sbu_segment IS NOT NULL
                  AND TRIM(attribute_sbu_segment) = '' THEN 1 ELSE 0 END)
        AS attribute_sbu_segment_empty,

    SUM(CASE WHEN attribute_send_to IS NOT NULL
                  AND TRIM(attribute_send_to) = '' THEN 1 ELSE 0 END)
        AS attribute_send_to_empty,

    SUM(CASE WHEN attribute_status_fica IS NOT NULL
                  AND TRIM(attribute_status_fica) = '' THEN 1 ELSE 0 END)
        AS attribute_status_fica_empty,

    SUM(CASE WHEN attribute_fic_complete IS NOT NULL
                  AND TRIM(attribute_fic_complete) = '' THEN 1 ELSE 0 END)
        AS attribute_fic_complete_empty,

    SUM(CASE WHEN call_data_default_ani IS NOT NULL
                  AND TRIM(call_data_default_ani) = '' THEN 1 ELSE 0 END)
        AS call_data_default_ani_empty,

    SUM(CASE WHEN call_data_cif_key IS NOT NULL
                  AND TRIM(call_data_cif_key) = '' THEN 1 ELSE 0 END)
        AS call_data_cif_key_empty,

    SUM(CASE WHEN call_data_id_number IS NOT NULL
                  AND TRIM(call_data_id_number) = '' THEN 1 ELSE 0 END)
        AS call_data_id_number_empty,

    SUM(CASE WHEN call_data_connection_status IS NOT NULL
                  AND TRIM(call_data_connection_status) = '' THEN 1 ELSE 0 END)
        AS call_data_connection_status_empty,

    SUM(CASE WHEN call_data_source_no IS NOT NULL
                  AND TRIM(call_data_source_no) = '' THEN 1 ELSE 0 END)
        AS call_data_source_no_empty,

    SUM(CASE WHEN call_data_destination_no IS NOT NULL
                  AND TRIM(call_data_destination_no) = '' THEN 1 ELSE 0 END)
        AS call_data_destination_no_empty,

    SUM(CASE WHEN call_data_queue_name IS NOT NULL
                  AND TRIM(call_data_queue_name) = '' THEN 1 ELSE 0 END)
        AS call_data_queue_name_empty,

    SUM(CASE WHEN call_data_connection_id IS NOT NULL
                  AND TRIM(call_data_connection_id) = '' THEN 1 ELSE 0 END)
        AS call_data_connection_id_empty,

    SUM(CASE WHEN call_data_context_id IS NOT NULL
                  AND TRIM(call_data_context_id) = '' THEN 1 ELSE 0 END)
        AS call_data_context_id_empty,

    SUM(CASE WHEN call_data_is_identified IS NOT NULL
                  AND TRIM(call_data_is_identified) = '' THEN 1 ELSE 0 END)
        AS call_data_is_identified_empty,

    SUM(CASE WHEN call_data_is_authenticated IS NOT NULL
                  AND TRIM(call_data_is_authenticated) = '' THEN 1 ELSE 0 END)
        AS call_data_is_authenticated_empty,

    SUM(CASE WHEN tag_billing_cost_center IS NOT NULL
                  AND TRIM(tag_billing_cost_center) = '' THEN 1 ELSE 0 END)
        AS tag_billing_cost_center_empty,

    SUM(CASE WHEN tag_billing_department IS NOT NULL
                  AND TRIM(tag_billing_department) = '' THEN 1 ELSE 0 END)
        AS tag_billing_department_empty,

    SUM(CASE WHEN tag_billing_division IS NOT NULL
                  AND TRIM(tag_billing_division) = '' THEN 1 ELSE 0 END)
        AS tag_billing_division_empty,

    SUM(CASE WHEN tag_speech_analytics IS NOT NULL
                  AND TRIM(tag_speech_analytics) = '' THEN 1 ELSE 0 END)
        AS tag_speech_analytics_empty,

    SUM(CASE WHEN tag_aws_connect_instance_id IS NOT NULL
                  AND TRIM(tag_aws_connect_instance_id) = '' THEN 1 ELSE 0 END)
        AS tag_aws_connect_instance_id_empty,

    SUM(CASE WHEN tag_aws_connect_system_endpoint IS NOT NULL
                  AND TRIM(tag_aws_connect_system_endpoint) = '' THEN 1 ELSE 0 END)
        AS tag_aws_connect_system_endpoint_empty
FROM public.ctr_flattened;






SELECT
    column_name,
    COUNT(*) AS empty_value_count
FROM public.ctr_flattened
UNPIVOT (
    column_value FOR column_name IN (
        aws_account_id,
        aws_contact_trace_record_format_version,
        contact_id,
        contact_association_id,
        initial_contact_id,
        previous_contact_id,
        next_contact_id,
        related_contact_id,
        instance_arn,
        channel,
        initiation_method,
        disconnect_reason,
        answering_machine_detection_status,
        agent_arn,
        agent_active_region,
        agent_username,
        agent_voice_enhancement_mode,
        agent_device_operating_system,
        agent_device_platform_name,
        agent_device_platform_version,
        agent_routing_profile_arn,
        agent_routing_profile_name,
        campaign_id,
        customer_endpoint_address,
        customer_endpoint_type,
        system_endpoint_address,
        system_endpoint_type,
        transferred_to_endpoint_address,
        transferred_to_endpoint_type,
        queue_arn,
        queue_name,
        recording_deletion_reason,
        recording_location,
        recording_status,
        recording_type,
        contact_lens_language_locale,
        contact_lens_redaction_behavior,
        contact_lens_redaction_mask_mode,
        contact_lens_redaction_policy,
        contact_lens_sentiment_behavior,
        attribute_analytics_provider,
        attribute_caller_cif_key,
        attribute_caller_id_number,
        attribute_caller_id_type,
        attribute_caller_name,
        attribute_caller_phone_number,
        attribute_contact_flow_id,
        attribute_context_manager_session_id,
        attribute_customer_number,
        attribute_is_analytics_enabled,
        attribute_is_authenticated,
        attribute_is_chat_analytics_enabled,
        attribute_is_identified,
        attribute_is_screen_recording_enabled,
        attribute_is_speech_analytics_enabled,
        attribute_is_survey_enabled,
        attribute_survey_id,
        attribute_account_no,
        attribute_all_linked_accounts,
        attribute_chosen_account_object,
        attribute_cif_key,
        attribute_client_group,
        attribute_contact_flow_name,
        attribute_customer_id,
        attribute_eval_return_code,
        attribute_from_telephone_banking,
        attribute_home_language_code,
        attribute_id_number,
        attribute_pin_type,
        attribute_registration_status,
        attribute_sbu_segment,
        attribute_send_to,
        attribute_status_fica,
        attribute_fic_complete,
        call_data_default_ani,
        call_data_cif_key,
        call_data_id_number,
        call_data_connection_status,
        call_data_source_no,
        call_data_destination_no,
        call_data_queue_name,
        call_data_connection_id,
        call_data_context_id,
        call_data_is_identified,
        call_data_is_authenticated,
        tag_billing_cost_center,
        tag_billing_department,
        tag_billing_division,
        tag_speech_analytics,
        tag_aws_connect_instance_id,
        tag_aws_connect_system_endpoint,
        segment_connect_subtype,
        segment_purpose_analytics_reference,
        segment_purpose_contact_search_reference,
        source_file
    )
)
WHERE TRIM(column_value) = ''
GROUP BY column_name
ORDER BY empty_value_count DESC, column_name;
