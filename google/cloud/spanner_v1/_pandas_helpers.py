import logging
import six

class pandas_df():
    
    def pd_dataframe(self):
        """Returns the response in a pandas dataframe of the StreamedResultSet object"""
        try :
            from pandas import DataFrame
        except ImportError:
            logging.error("Pandas module not found. It is needed for converting query result to Dataframe.\n Try running 'pip3 install pandas'")

        code_to_spanner_dtype_dict = {
                                        1 : 'BOOL',
                                        2 : 'INT64',
                                        3 : 'FLOAT64',
                                        4 : 'TIMESTAMP',
                                        5 : 'DATE',
                                        6 : 'STRING',
                                        7 : 'BYTES',
                                        8 : 'ARRAY',
                                        10 : 'NUMERIC'
                                    }
        response = six.next(self._response_iterator)
        if self._metadata is None:  # first response
            metadata = self._metadata = response.metadata
            
        #Creating dictionary to store column name maping of spanner to pandas dataframe
        columns_dict={}
        try :
            for item in metadata.row_type.fields :
                columns_dict[item.name]=code_to_spanner_dtype_dict[item.type_.code]
        except :
            logging.warning("Not able to create spanner to pandas fields mapping")

        #Creating list of columns to be mapped with the data
        column_list=[k for k,v in columns_dict.items()]

        #Creating list of data values to be converted to dataframe
        values = list(response.values)
        if self._pending_chunk is not None:
            values[0] = self._merge_chunk(values[0])
        if response.chunked_value:
            self._pending_chunk = values.pop()
        self._merge_values(values)

        width = len(column_list)

        # list to store each row as a sub-list
        data=[] 
        while len(values)/width > 0 :
            data.append(values[:width])
            values=values[width:]

        #Creating dataframe using column headers and list of data rows
        df = DataFrame(data,columns=column_list)

        #Mapping dictionary to map every spanner datatype to a pandas compatible datatype
        mapping_dict={
                    'INT64':'int64',
                    'STRING':'object',
                    'BOOL':'bool',
                    'BYTES':'object', 
                    'ARRAY':'object',
                    'DATE':'datetime64[ns, UTC]',
                    'FLOAT64':'float64', 
                    'NUMERIC':'object', 
                    'TIMESTAMP':'datetime64[ns, UTC]'
                    }
        for k,v in columns_dict.items() :
            try:
                df[k]= df[k].astype(mapping_dict[v])
            except KeyError:
                print("Spanner Datatype not present in datatype mapping dictionary")
    
        return df