##########################################################################
### TODO WAITING ON https://stitchdata.atlassian.net/browse/SRCE-5065
##########################################################################

# import os

# from datetime import timedelta
# from datetime import datetime as dt

# from  tap_tester import connections, runner

# from base import GoogleAnalyticsBaseTest


# class GoogleAnalyticsPaginationTest(GoogleAnalyticsBaseTest):
#     """Test that we are paginating for streams when exceeding the API record limit of a single query"""

#     @staticmethod
#     def name():
#         return "tap_tester_google_analytics_pagination_test"


#     def test_run(self):
#         """
#         Verify that for each report you can get multiple pages of data for a given date.

#         PREREQUISITE
#         For EACH report, a dimension must be selected in which the number of distinct values is
#         greater than the default value of maxResults (page size).
#         """
#         self.start_date = (dt.utcnow() - timedelta(days=4)).strftime(self.START_DATE_FORMAT) # Needs to be prior to isGolden..
#         API_LIMIT = {stream: 1000 for stream in self.expected_sync_streams()}
#         expected_streams = {'Audience Overview', 'report 1'}  # self.expected_sync_streams()

#         # Create connection but do not use default start date
#         conn_id = connections.ensure_connection(self, original_properties=False)

#         # run check mode
#         found_catalogs = self.run_and_verify_check_mode(conn_id)

#         # TODO remove once all streams covered
#         found_catalogs = [catalog for catalog in found_catalogs
#                           if catalog.get('stream_name') in expected_streams]

#         # TODO select specific streams
#         # table and field selection
#         self.perform_and_verify_table_and_field_selection(
#             conn_id, found_catalogs, select_all_fields=True,
#             #select_default_fields=False, select_pagination_fields=True
#         )


#         # run initial sync
#         record_count_by_stream = self.run_and_verify_sync(conn_id)

#         synced_records = runner.get_records_from_target_output()
#         import pdb; pdb.set_trace()
#         #  messages1 = synced_records['Audience Overview']['messages']
#         messages2 = synced_records['report 1']['messages']

#         # m1_25 = [m for m in messages1 if m['data']['start_date'] == '2021-02-25T00:00:00.000000Z' ]
#         # m1_26 = [m for m in messages1 if m['data']['start_date'] == '2021-02-26T00:00:00.000000Z' ]
#         # m1_27 = [m for m in messages1 if m['data']['start_date'] == '2021-02-27T00:00:00.000000Z' ]
#         # m1_28 = [m for m in messages1 if m['data']['start_date'] == '2021-02-28T00:00:00.000000Z' ]

#         m2_25 = [m for m in messages2 if m['data']['start_date'] == '2021-02-25T00:00:00.000000Z' ]
#         m2_26 = [m for m in messages2 if m['data']['start_date'] == '2021-02-26T00:00:00.000000Z' ]
#         m2_27 = [m for m in messages2 if m['data']['start_date'] == '2021-02-27T00:00:00.000000Z' ]
#         m2_28 = [m for m in messages2 if m['data']['start_date'] == '2021-02-28T00:00:00.000000Z' ]

#         for stream in expected_streams:
#             with self.subTest(stream=stream):

#                 # TODO Verify we are paginating for testable synced streams
#                 self.assertGreater(record_count_by_stream.get(stream, -1), API_LIMIT.get(stream),
#                                    msg="We didn't guarantee pagination. The number of records should exceed the api limit.")

#                 data = synced_records.get(stream, [])
#                 actual_records = [row['data'] for row in data['messages']]
#                 record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
#                 auto_fields = self.expected_automatic_fields().get(stream)

#                 for actual_keys in record_messages_keys:

#                     # Verify that the automatic fields are sent to the target for paginated streams
#                     self.assertTrue(auto_fields.issubset(actual_keys),
#                                     msg="A paginated synced stream has a record that is missing automatic fields.")

#                     # Verify we have more fields sent to the target than just automatic fields (this is set above)
#                     self.assertEqual(set(), auto_fields.difference(actual_keys),
#                                      msg="A paginated synced stream has a record that is missing expected fields.")

#                 # TODO Verify by pks that the replicated records match our expectations
#                 # self.assertPKsEqual(stream, expected_records.get(stream), actual_records)
