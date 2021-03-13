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

import logging
import warnings


def pd_dataframe(result_set):
    """This functions converts the query results into pandas dataframe

    :rtype: pandas.Dataframe
    :returns: Dataframe with the help of a mapping dictionary which maps every spanner datatype to a pandas compatible datatype.
    """
    try:
        from pandas import DataFrame
    except ImportError as err:
        raise ImportError(
            "Pandas module not found. It is needed for converting query result to Dataframe.\n Try running 'pip3 install pandas'"
        ) from err

    data = list(result_set)

    columns_dict = {}
    for item in result_set.fields:
        columns_dict[item.name] = item.type_.code

    # Creating list of columns to be mapped with the data
    column_list = list(columns_dict.keys())

    # Creating dataframe using column headers and list of data rows
    df = DataFrame(data, columns=column_list)

    for k, v in columns_dict.items():
        if v.name == "TIMESTAMP" or v.name == "DATE":
            df[k] = df[k].dt.tz_localize("UTC")


    return df
