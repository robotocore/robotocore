        // -- Phone numbers ----------------------------------------------------
        "ClaimPhoneNumber" => op_claim_phone_number(st, body, &req.region, req.account),
        "DescribePhoneNumber" => op_describe_phone_number(st, param(params, "PhoneNumberId")),
        "UpdatePhoneNumber" => op_update_phone_number(st, param(params, "PhoneNumberId"), body),
        "ReleasePhoneNumber" => op_release_phone_number(st, param(params, "PhoneNumberId")),
        "ListPhoneNumbers" => op_list_phone_numbers(st, param(params, "InstanceId")),
        "ListPhoneNumbersV2" => op_list_phone_numbers_v2(st),
        "SearchAvailablePhoneNumbers" => op_search_available_phone_numbers(st, body),
        // -- Prompts ------------------------------------------------------------
        "CreatePrompt" => op_create_prompt(st, param(params, "InstanceId"), body),
        "DescribePrompt" => {
            op_describe_prompt(st, param(params, "InstanceId"), param(params, "PromptId"))
        }
        "UpdatePrompt" => {
            op_update_prompt(st, param(params, "InstanceId"), param(params, "PromptId"), body)
        }
        "DeletePrompt" => {
            op_delete_prompt(st, param(params, "InstanceId"), param(params, "PromptId"))
        }
        "ListPrompts" => op_list_prompts(st, param(params, "InstanceId")),
        // -- Views ---------------------------------------------------------------
        "CreateView" => op_create_view(st, param(params, "InstanceId"), body),
        "DescribeView" => op_describe_view(st, param(params, "InstanceId"), param(params, "ViewId")),
        "UpdateViewContent" => {
            op_update_view_content(st, param(params, "InstanceId"), param(params, "ViewId"), body)
        }
        "UpdateViewMetadata" => {
            op_update_view_metadata(st, param(params, "InstanceId"), param(params, "ViewId"), body)
        }
        "DeleteView" => op_delete_view(st, param(params, "InstanceId"), param(params, "ViewId")),
        "ListViews" => op_list_views(st, param(params, "InstanceId")),
        // -- Vocabularies -----------------------------------------------------------
        "CreateVocabulary" => op_create_vocabulary(st, param(params, "InstanceId"), body),
        "DescribeVocabulary" => {
            op_describe_vocabulary(st, param(params, "InstanceId"), param(params, "VocabularyId"))
        }
        "DeleteVocabulary" => {
            op_delete_vocabulary(st, param(params, "InstanceId"), param(params, "VocabularyId"))
        }
        "SearchVocabularies" => op_search_vocabularies(st, param(params, "InstanceId"), body),
        "ListDefaultVocabularies" => op_list_default_vocabularies(st, param(params, "InstanceId")),
        // -- Rules -----------------------------------------------------------------
        "CreateRule" => op_create_rule(st, param(params, "InstanceId"), body),
        "DescribeRule" => op_describe_rule(st, param(params, "InstanceId"), param(params, "RuleId")),
        "UpdateRule" => op_update_rule(st, param(params, "InstanceId"), param(params, "RuleId"), body),
        "DeleteRule" => op_delete_rule(st, param(params, "InstanceId"), param(params, "RuleId")),
        "ListRules" => op_list_rules(st, param(params, "InstanceId")),
        // -- Evaluation forms --------------------------------------------------------
        "CreateEvaluationForm" => op_create_evaluation_form(st, param(params, "InstanceId"), body),
        "DescribeEvaluationForm" => op_describe_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        "UpdateEvaluationForm" => op_update_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            body,
        ),
        "ActivateEvaluationForm" => op_activate_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            true,
        ),
        "DeactivateEvaluationForm" => op_activate_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            false,
        ),
        "DeleteEvaluationForm" => op_delete_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        "ListEvaluationForms" => op_list_evaluation_forms(st, param(params, "InstanceId")),
        "ListEvaluationFormVersions" => op_list_evaluation_form_versions(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        // -- Predefined attributes -----------------------------------------------------
        "CreatePredefinedAttribute" => {
            op_create_predefined_attribute(st, param(params, "InstanceId"), body)
        }
        "DescribePredefinedAttribute" => op_describe_predefined_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "Name"),
        ),
        "UpdatePredefinedAttribute" => op_update_predefined_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "Name"),
            body,
        ),
        "DeletePredefinedAttribute" => op_delete_predefined_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "Name"),
        ),
        "ListPredefinedAttributes" => op_list_predefined_attributes(st, param(params, "InstanceId")),
        // -- Task templates ---------------------------------------------------------------
        "CreateTaskTemplate" => op_create_task_template(st, param(params, "InstanceId"), body),
        "GetTaskTemplate" => op_get_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
        ),
        "UpdateTaskTemplate" => op_update_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
            body,
        ),
        "DeleteTaskTemplate" => op_delete_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
        ),
        "ListTaskTemplates" => op_list_task_templates(st, param(params, "InstanceId")),
        // -- Integration associations + use cases ------------------------------------------
        "CreateIntegrationAssociation" => {
            op_create_integration_association(st, param(params, "InstanceId"), body)
        }
        "DeleteIntegrationAssociation" => op_delete_integration_association(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
        ),
        "ListIntegrationAssociations" => {
            op_list_integration_associations(st, param(params, "InstanceId"))
        }
        "CreateUseCase" => op_create_use_case(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
            body,
        ),
        "DeleteUseCase" => op_delete_use_case(
            st,
            param(params, "InstanceId"),
            param(params, "UseCaseId"),
        ),
        "ListUseCases" => op_list_use_cases(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
        ),
        // -- Email addresses -------------------------------------------------------------
        "CreateEmailAddress" => op_create_email_address(st, param(params, "InstanceId"), body),
        "DescribeEmailAddress" => op_describe_email_address(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
        ),
        "DeleteEmailAddress" => op_delete_email_address(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
        ),
        // -- Notifications ---------------------------------------------------------------
        "CreateNotification" => op_create_notification(st, param(params, "InstanceId"), body),
        "DescribeNotification" => op_describe_notification(
            st,
            param(params, "InstanceId"),
            param(params, "NotificationId"),
        ),
        "DeleteNotification" => op_delete_notification(
            st,
            param(params, "InstanceId"),
            param(params, "NotificationId"),
        ),
        "ListNotifications" => op_list_notifications(st, param(params, "InstanceId")),
        // -- Workspaces ---------------------------------------------------------------------
        "CreateWorkspace" => op_create_workspace(st, param(params, "InstanceId"), body),
        "DescribeWorkspace" => op_describe_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "DeleteWorkspace" => op_delete_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "ListWorkspaces" => op_list_workspaces(st, param(params, "InstanceId")),
        // -- Test cases --------------------------------------------------------------------
        "CreateTestCase" => op_create_test_case(st, param(params, "InstanceId"), body),
        "DescribeTestCase" => {
            op_describe_test_case(st, param(params, "InstanceId"), param(params, "TestCaseId"))
        }
        "UpdateTestCase" => op_update_test_case(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
            body,
        ),
        "DeleteTestCase" => {
            op_delete_test_case(st, param(params, "InstanceId"), param(params, "TestCaseId"))
        }
        "ListTestCases" => op_list_test_cases(st, param(params, "InstanceId")),
        // -- Data tables --------------------------------------------------------------------
        "CreateDataTable" => op_create_data_table(st, param(params, "InstanceId"), body),
        "DescribeDataTable" => {
            op_describe_data_table(st, param(params, "InstanceId"), param(params, "DataTableId"))
        }
        "DeleteDataTable" => {
            op_delete_data_table(st, param(params, "InstanceId"), param(params, "DataTableId"))
        }
        "ListDataTables" => op_list_data_tables(st, param(params, "InstanceId")),
        "CreateDataTableAttribute" => op_create_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            body,
        ),
        "DescribeDataTableAttribute" => op_describe_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
        ),
        "ListDataTableAttributes" => op_list_data_table_attributes(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
        ),
        "UpdateDataTableAttribute" => op_update_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
            body,
        ),
        "DeleteDataTableAttribute" => op_delete_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
        ),
        // -- Traffic distribution groups ----------------------------------------------------
        "CreateTrafficDistributionGroup" => {
            op_create_traffic_distribution_group(st, body, &req.region, req.account)
        }
        "DescribeTrafficDistributionGroup" => op_describe_traffic_distribution_group(
            st,
            param(params, "TrafficDistributionGroupId"),
        ),
        "DeleteTrafficDistributionGroup" => op_delete_traffic_distribution_group(
            st,
            param(params, "TrafficDistributionGroupId"),
        ),
        "ListTrafficDistributionGroups" => op_list_traffic_distribution_groups(st, query),
