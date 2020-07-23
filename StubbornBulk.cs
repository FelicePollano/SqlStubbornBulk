using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace SqlStubbornBulk
{
    public class StubbornBulk
    {
        readonly DataTable data;
        private readonly SqlConnection connection;
        readonly string destinationTable;
        readonly int batchSize;
        readonly string countCommand;
        public StubbornBulk(DataTable data,SqlConnection connection,string destinationTable,int batchSize=1000)
        {
           
            this.data = data ?? throw new ArgumentNullException(nameof(data));
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.destinationTable = destinationTable ?? throw new ArgumentNullException(nameof(destinationTable));
            this.batchSize = batchSize;
            this.countCommand = $"select count (*) from {destinationTable}"; //not dealing with query injection, this string does not come from UI
        }
        /// <summary>
        /// Perform the bulk, returning a datatable with remaining rows, 
        /// these rows are not inserted due to some errors
        /// </summary>
        /// <param name="failedRows">a collection of rows that are failed, with the exception causing the failure</param>
        /// <returns>the remaining data</returns>
        private DataTable PerformBulkInternal(List<FailedRow> failedRows)
        {
            if (failedRows is null)
            {
                throw new ArgumentNullException(nameof(failedRows));
            }

            int countBefore = 0;
            DataTable remaining = data.Clone(); // obtain a datatable with same schema
            SqlCommand cmd = new SqlCommand(this.countCommand);
            cmd.Connection = connection;
            //error here are not recoverable, so we let the exception pop out to the caller    
            countBefore = System.Convert.ToInt32(cmd.ExecuteScalar());
           
            var bulk = new SqlBulkCopy(connection);
            try{
                bulk.BatchSize = batchSize;
                bulk.WriteToServer(data);
            }catch( SqlException e)
            {
                //we handle just data error, we need to be careful here in order to avoid infinitive loop
                int countAfter = System.Convert.ToInt32(cmd.ExecuteScalar());
                if(countAfter == countBefore) // the first line is failing
                {
                    var failed = new FailedRow(){ Row = data.Rows[0],Exception = e};
                    failedRows.Add(failed);
                }
                //add the remaining rows to remaining table
                var sindex = countAfter-countBefore+1;
                for(int i=sindex;i<data.Rows.Count;++i)
                {
                    remaining.ImportRow(data.Rows[i]);
                }
                return remaining;
            }
            //if we get here, we eventually bulk all the lines, so we return an empty table
            return remaining;
        }
        /// <summary>
        /// Perform a bulk, doing many attempts in order to put as many rows as possible
        /// and colect the failures
        /// </summary>
        /// <param name="failedRows"> a collection that will be filled with failures</param>
        public void PerformBulk(List<FailedRow> failedRows)
        {
            if (failedRows is null)
            {
                throw new ArgumentNullException(nameof(failedRows));
            }
            DataTable remaining;
            do{
                remaining = PerformBulkInternal(failedRows);
            }
            while(remaining.Rows.Count == 0);
        }

    }
}
