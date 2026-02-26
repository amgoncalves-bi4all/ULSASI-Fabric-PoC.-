# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

json_config_test_af = {
  "actiontype": "Create",
  "location": "https://eu1analyticsst01test.dfs.core.windows.net",
  "lakehouse_id" : "ae9fc0c6-726d-498a-b50f-cfebed1b26f4",
  "connectionid": "6b245079-583d-49ee-986b-75e7c0e9f43a",
  "shortcut_definition": [
                {"name": "dim_aircraft_registration", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/0066f305-7076-4071-bf0d-46be75cba941"},
                {"name": "dim_aircraft_type", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/0385f6aa-775a-4280-b7ca-e88756e85bb2"},
                {"name": "dim_airports_arrivals", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/96a961ea-25bb-46c7-9f10-31ba567d213e"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/96a961ea-25bb-46c7-9f10-31ba567d213e"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/96a961ea-25bb-46c7-9f10-31ba567d213e"},
                {"name": "dim_airports_departures", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/96a961ea-25bb-46c7-9f10-31ba567d213e"},
                {"name": "dim_airports_departures_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/96a961ea-25bb-46c7-9f10-31ba567d213e"},
                {"name": "dim_boolean", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/445e0678-513f-4d96-b949-b8c58fb56cb8"},
                {"name": "dim_codeshare", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/74de4022-2b27-4cb9-914a-3666dfadd5f3"},
                {"name": "dim_date_created", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_ldt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_ldt_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_sta", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_sta_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_sta_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_sta_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_std", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_std_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_std_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_std_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_tot_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_tot_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_updated", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_version_end", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_date_version_start", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/9c6d0430-afb0-48d9-aafd-7d9a887ea98b"},
                {"name": "dim_flight", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/3e481c09-ece9-4cf4-963f-f35e5cf961ca"},
                {"name": "dim_gate_arrivals", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/d8fff324-0760-46a2-bd17-44a1f1c10da4"},
                {"name": "dim_gate_departures", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/d8fff324-0760-46a2-bd17-44a1f1c10da4"},
                {"name": "dim_internal_status", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/692f5a96-eb19-4c3c-8978-5ca161ef82e4"},
                {"name": "dim_operational_suffix", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7f03b225-bd56-4917-a2bd-f283de0706f0"},
                {"name": "dim_owner" ,"subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/575b4046-1f09-4862-b85e-c88368cda1ec"},
                {"name": "dim_time_created", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_ldt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_ldt_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_sta_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_sta", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_sta_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_sta_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_std", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_std_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_std_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_std_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_tot_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_tot_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_updated", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_version_start", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_time_version_end", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/7b4c9bf5-0b69-4320-a306-095c76b5ea3b"},
                {"name": "dim_travel_classes", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/dc2000cf-a5e6-4d68-8721-bbb950d75b14"},
                {"name": "fact_flight", "subpath": "/analyticsaf/gold/__unitystorage/schemas/a8c2dc10-df91-48b4-8744-cecf79b5a8b4/tables/3d44769c-6a32-40ac-9a8e-65d3403a47c3"}
                ]
}

#json_config_test_af

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_config_prod_af = {
  "actiontype": "Create",
  "location": "https://eu1analyticsst01prod.dfs.core.windows.net",
  "lakehouse_id" : "ae9fc0c6-726d-498a-b50f-cfebed1b26f4",
  "connectionid": "",
  "shortcut_definition": [
                {"name": "dim_aircraft_registration", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/b61fbbe4-354f-4aac-b309-cbe47b466c9b"},
                {"name": "dim_aircraft_type", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/28201b62-d220-4154-801f-ae244eda0d3b"},
                {"name": "dim_airports_arrivals", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_departures", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_departures_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_boolean", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/6a9c531d-be33-4ffc-aac0-e89f4d77f953"},
                {"name": "dim_codeshare", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/be10104b-3ef7-48c4-8fd6-4d8a15d8be75"},
                {"name": "dim_date_created", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_ldt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_ldt_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_tot_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_tot_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_updated", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_version_end", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_version_start", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_flight", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/f35935d6-0358-4901-a2c8-ae855a5b0ecc"},
                {"name": "dim_gate_arrivals", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/e57f443d-273a-4108-834a-a827410d743a"},
                {"name": "dim_gate_departures", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/e57f443d-273a-4108-834a-a827410d743a"},
                {"name": "dim_internal_status", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/75ffd532-4c56-418e-bad2-10a9796d0c22"},
                {"name": "dim_operational_suffix", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/0f0384ec-aed4-44dc-b817-9a005f2b1028"},
                {"name": "dim_owner" ,"subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/e4aed199-e70d-44a9-99f9-7e248c59542b"},
                {"name": "dim_time_created", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_ldt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_ldt_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_lt", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_tot_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_tot_lt_eff", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_updated", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_version_start", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_version_end", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_travel_classes", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/8d10e3cf-ce9c-44aa-9807-2b4399cd09b3"},
                {"name": "fact_flight", "subpath": "/analyticsaf/gold/__unitystorage/schemas/25fa320a-6c3b-40c5-aef8-a9fd4fcd33f1/tables/5c2eb9a2-7f78-4ff5-8737-1b495fca4c2a"}
                ]
}

#json_config_prod_af

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_config_test_qf = {
  "actiontype": "Create",
  "location": "https://aus1analyticsst01test.dfs.core.windows.net",
  "lakehouse_id" : "53147fa8-1fae-4380-a5a4-aefd2bbf53c1",
  "connectionid": "",
  "shortcut_definition": [
                {"name": "dim_aircraft_registration", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/b61fbbe4-354f-4aac-b309-cbe47b466c9b"},
                {"name": "dim_aircraft_type", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/28201b62-d220-4154-801f-ae244eda0d3b"},
                {"name": "dim_airports_arrivals", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_departures", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_airports_departures_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/39124505-df1a-41f7-8f40-9be65045b902"},
                {"name": "dim_boolean", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/6a9c531d-be33-4ffc-aac0-e89f4d77f953"},
                {"name": "dim_codeshare", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/be10104b-3ef7-48c4-8fd6-4d8a15d8be75"},
                {"name": "dim_date_created", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_ldt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_ldt_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_sta_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_std_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_tot_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_tot_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_updated", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_version_end", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_date_version_start", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/9378142f-ae05-4584-a4ee-32a0f4c030fa"},
                {"name": "dim_flight", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/f35935d6-0358-4901-a2c8-ae855a5b0ecc"},
                {"name": "dim_gate_arrivals", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/e57f443d-273a-4108-834a-a827410d743a"},
                {"name": "dim_gate_departures", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/e57f443d-273a-4108-834a-a827410d743a"},
                {"name": "dim_internal_status", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/75ffd532-4c56-418e-bad2-10a9796d0c22"},
                {"name": "dim_operational_suffix", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/0f0384ec-aed4-44dc-b817-9a005f2b1028"},
                {"name": "dim_owner" ,"subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/e4aed199-e70d-44a9-99f9-7e248c59542b"},
                {"name": "dim_time_created", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_ldt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_ldt_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_sta_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_std_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_tot_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_tot_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_updated", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_version_start", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_time_version_end", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/4097b5ba-e266-4dad-b2d4-ea0ad8c78a84"},
                {"name": "dim_travel_classes", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/8d10e3cf-ce9c-44aa-9807-2b4399cd09b3"},
                {"name": "fact_flight", "subpath": "/analyticsqf/gold/__unitystorage/schemas/73f06284-06d0-4448-83a8-a72a882575d7/tables/5c2eb9a2-7f78-4ff5-8737-1b495fca4c2a"}
                ]
}

#json_config_test_qf 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_config_prod_qf = {
  "actiontype": "Create",
  "location": "https://aus1analyticsst01prod.dfs.core.windows.net",
  "lakehouse_id" : "3c078514-5a70-4046-a568-173977d2ee48",
  "connectionid": "",
  "shortcut_definition": [
                {"name": "dim_aircraft_registration", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/02479508-4d66-4497-8b45-c69abc2b7b23"},
                {"name": "dim_aircraft_type", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/702474b9-89e8-4eab-982d-baf93a85846e"},
                {"name": "dim_airports_arrivals", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd39d079-c7d2-4bbd-a673-fdfe3ad6d443"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd39d079-c7d2-4bbd-a673-fdfe3ad6d443"},
                {"name": "dim_airports_arrivals_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd39d079-c7d2-4bbd-a673-fdfe3ad6d443"},
                {"name": "dim_airports_departures", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd39d079-c7d2-4bbd-a673-fdfe3ad6d443"},
                {"name": "dim_airports_departures_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd39d079-c7d2-4bbd-a673-fdfe3ad6d443"},
                {"name": "dim_boolean", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd9d0a10-5dc9-454f-821d-b880712357a3"},
                {"name": "dim_codeshare", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/dd32939d-52df-4571-9022-fad271189ad0"},
                {"name": "dim_date_created", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_ldt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_ldt_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_sta", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_sta_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_sta_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_sta_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_std", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_std_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_std_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_std_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_tot_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_tot_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_updated", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_version_end", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_date_version_start", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/eebe057c-04b6-4588-9793-e132241d3d32"},
                {"name": "dim_flight", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/35ae4b15-bf34-4cfe-b486-50719c0b5986"},
                {"name": "dim_gate_arrivals", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/9b131ae3-047f-4e58-a0fa-0fc1df0ddf11"},
                {"name": "dim_gate_departures", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/9b131ae3-047f-4e58-a0fa-0fc1df0ddf11"},
                {"name": "dim_internal_status", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/7b6e0544-abc0-4b5b-baa7-7613d43fa4a9"},
                {"name": "dim_operational_suffix", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/cb21f58d-179b-4287-a757-4d15a4a13102"},
                {"name": "dim_owner", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/fe64e30e-d652-40ea-b91a-a3a5619f6b83"},
                {"name": "dim_time_created", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_ldt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_ldt_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_sta_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_sta", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_sta_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_sta_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_std", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_std_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_std_lt", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_std_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_tot_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_tot_lt_eff", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_updated", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_version_start", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_time_version_end", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a76b422c-2f79-4365-80d1-939f860b0f59"},
                {"name": "dim_travel_classes", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a6ce9388-b408-4634-b13d-cf024a109fb0"},
                {"name": "fact_flight", "subpath": "/analyticsqf/gold/__unitystorage/schemas/a6a9e68d-83d1-419d-8291-7aab5664a2f8/tables/a11edd01-ce5a-47c8-9f7f-71f50d3aa3e2"}
                ]
}

#json_config_prod_qf 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import json
import requests
from requests import status_codes
import sempy.fabric as fabric
import time
import pandas as pd
workspace_id = fabric.get_workspace_id()

#df = pd.read_json(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{config_file_path}",typ="series")
df = pd.DataFrame(json_config_test_af)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fn_shortcut(action, shortcut_path, shortcut_name, lakehouse_id , source = None):

    request_headers = {
        "Authorization": "Bearer " + mssparkutils.credentials.getToken("pbi"),
        "Content-Type": "application/json"
    }
    #print(request_headers)

    request_body = {
        "path": shortcut_path,
        "name": shortcut_name,
        "target": source
    }
    print(request_body)


    # Get a shortcut
    if action == 'Get':
        response = requests.request(method = "GET", url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/{shortcut_path}/{shortcut_name}', headers = request_headers)

    # Create a shortcut
    if action == 'Create':
        response = requests.request(method = "POST", url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts?shortcutConflictPolicy=Abort', headers = request_headers, json = request_body)

    # Delete a shortcut
    if action == 'Delete':
        response = requests.request(method = "DELETE", url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/{shortcut_path}/{shortcut_name}', headers = request_headers)

        if response.status_code == 200:
            # Wait for the delete operation to fully propogate
            while mssparkutils.fs.exists(f'{shortcut_path}/{shortcut_name}'):
                time.sleep(5)

    print(response.status_code)
    # Build the return payload for a success response
    if (response.status_code >= 200 and response.status_code <= 299):
        response_content = {
            "request_url"           : response.url,
            "response_content"      : {} if response.text == '' else json.loads(response.text),
            "status"                : "success",
            "status_code"           : response.status_code,
            "status_description"    : status_codes._codes[response.status_code][0]
            }

    # Build the return payload for a failure response
    if not (response.status_code >= 200 and response.status_code <= 299):
        response_content = {
            "request_body"          : request_body,
            "request_headers"       : request_headers,
            "request_url"           : response.url,
            "response_text"         : json.loads(response.text),
            "status"                : "error",
            "status_code"           : response.status_code,
            "status_description"    : status_codes._codes[response.status_code][0]
        }

    return response_content


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

    for i, r in df.head(1).iterrows():
        ActionType = r['actiontype']
        Location = r['location']
        Lakehouse = r['lakehouse_id']
        ConnectionId = r['connectionid']
        shortcut_definition = r['shortcut_definition']
        shortcut_name = r['shortcut_definition']['name']
        shortcut_subpath = r['shortcut_definition']['subpath']
        print(f"Index: {i}, Location: {r['location']}, Lakehouse: {r['lakehouse_id']}, shortcut_name: {r['shortcut_definition']['name']}")

        if ActionType=='Create':
            source = {
            "adlsGen2": {
                "location": f"{Location}",
                "subpath": f"{r['shortcut_definition']['subpath']}",
                "connectionId": f"{ConnectionId}"
            }}
            fn_shortcut(action = 'Create', shortcut_path = 'Tables/gold', shortcut_name = shortcut_name, lakehouse_id = Lakehouse , source = source)
        # Or make the output pretty with this command
        #print(json.dumps(fn_shortcut(action = 'Create', shortcut_path = 'Tables/gold', shortcut_name = shortcut_name, lakehouse_id = Lakehouse , source = source), indent = 5))
     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# not tested

#if ActionType=='Get':
#    for name in ShortcutNames:
#        responsecontent=fn_shortcut("Get", "Tables", name)
#        print(responsecontent)
#if ActionType=='Delete':
#    for name in ShortcutNames:
#        fn_shortcut("Delete", "Tables", name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
