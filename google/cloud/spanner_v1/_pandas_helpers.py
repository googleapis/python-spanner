# Copyright 2021 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helper module for working with the pandas library."""

try:
    import pandas

    pandas_import_error = None
except ImportError as err:
    pandas = None
    pandas_import_error = err


def check_pandas_import():
    if pandas is None:
        raise ImportError(
            "The pandas module is required for this method.\n"
            "Try running 'pip3 install pandas'"
        ) from pandas_import_error


def to_dataframe(result_set):
    """This functions converts the query results into pandas dataframe

    :type result_set: :class:`~google.cloud.spanner_v1.StreamedResultSet`
    :param result_set: complete response data returned from a read/query

    :rtype: pandas.DataFrame
    :returns: Dataframe with the help of a mapping dictionary which maps every spanner datatype to a pandas compatible datatype.
    """
    check_pandas_import()

    # Download all results first, so that the fields property is populated.
    data = list(result_set)

    columns_dict = {}
    column_list = []
    for item in result_set.fields:
        column_list.append(item.name)
        columns_dict[item.name] = item.type_.code

    # Creating dataframe using column headers and list of data rows
    df = pandas.DataFrame(data, columns=column_list)

    # Convert TIMESTAMP and DATE columns to appropriate type. The
    # datetime64[ns, UTC] dtype is null-safe.
    for k, v in columns_dict.items():
        if v.name == "TIMESTAMP" or v.name == "DATE":
            try:
                df[k] = df[k].to_datetime()
            except Exception as e:
                df[k]=df[k].astype('datetime64[ns]')
                df[k]=df[k].dt.tz_localize("UTC")

    return df
