package com.deltalake.hudi.comparison

import java.sql.Timestamp
import java.time.LocalDateTime

case class DatasetForTesting(year:Integer,month:Integer,
                             pk_1:String,field_1:Double=0.0,field_2:Double=0.0,field_3:Double=0.0,field_4:Double=0.0,
                             field_5:Double=0.0, field_6:Double=0.0,field_7:Double=0.0,field_8:Double=0.0,field_9:Double=0.0,
                             create_date:Timestamp=Timestamp.valueOf(LocalDateTime.now()))

