using System;
using System.Data;

namespace SqlStubbornBulk
{
    public class FailedRow
    {
        public DataRow Row { get; set; }
        public Exception Exception { get; set; }
    }
}