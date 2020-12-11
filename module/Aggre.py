# import os
#from qgis.core import *
from qgis.core import (
  QgsGeometry,
QgsField,
  QgsPointXY,
  QgsFeature,
  QgsProcessingFeedback,
  QgsVectorLayer,
  QgsVectorFileWriter,
  QgsCoordinateReferenceSystem,
  QgsWkbTypes

)
from PyQt5.QtCore import    QVariant
#  sample program of running processing 
import sys
#from qgis.analysis import QgsNativeAlgorithms
#import os.path
import processing

import csv
import sqlite3

import uuid

import codecs


#from processing.core.Processing import Processing




#    レイヤ EPSG変換 再投影
#
#    ChangeEPSG( input, outputDB, outputLayer, Epsg )
#
#     inputfile   SJIS ファイル名
#     tempdir     作業ファイル出力ディレクトリ
#     output      変換後ファイル名
#
def  ChangeEPSG( inputL, outputDB, outputLayer, Epsg ):



     output_tbl4 = 'ogr:dbname=\'' +  outputDB + '\' table=\"' + outputLayer +  '\" (geom) sql='

     params4 ={ 'INPUT' : inputL, 'OUTPUT' : output_tbl4, 'TARGET_CRS' : QgsCoordinateReferenceSystem(Epsg) }

     feedback = QgsProcessingFeedback()
     
     res = processing.run('native:reprojectlayer', params4, feedback=feedback)

     return( outputLayer )
     

#    SJIS text file の UTF8変換
#
#    CnvSJIS2UTF8( inputfile , tempdir)
#
#     inputfile   SJIS ファイル名
#     tempdir     作業ファイル出力ディレクトリ
#     output      変換後ファイル名
#
def CnvSJIS2UTF8( inputfile, tempdir ):

     file_id = str(uuid.uuid4())

     # UTF-8 ファイルのパス
     utf_8_file  = tempdir + "/" + file_id

     # 文字コードを utf-8 に変換して保存
     fin = codecs.open(inputfile, "r", "shift_jis")
     fout_utf = codecs.open(utf_8_file , "w", "utf-8")
     for row in fin:
          fout_utf.write(row)
     fin.close()
     fout_utf.close()
     # UTF-8 ファイルのパス
   
     return( utf_8_file )


#    ポリゴン間のInterSect（交差） の実行
#
#    ExecuteInterSect( mlayer, rlayer, output_tbl2  )
#
#   mlayer    メッシュの元レイヤ
#   rlayer    行政界別集計レイヤ
#   output_tbl2   出力テーブル定義
#           output_tbl2 = 'ogr:dbname=\'' + outputdb + '\' table=\"intersect' +  hogehoge_id +  '\" (geom) sql='
#    
#
def  ExecuteInterSect( mlayer, rlayer, output_tbl2  ):

     feedback = QgsProcessingFeedback()
     
     params2 = { 'INPUT' : mlayer, 'INPUT_FIELDS' : [], 'OUTPUT' : output_tbl2, 'OVERLAY' : rlayer, 'OVERLAY_FIELDS' : [] }

     res = processing.run('native:intersection', params2, feedback=feedback)

     #print( output_tbl2 )
     return(  res )
     
     

#  CSVファイルのロードと行政区域別集計
#
#   ImportandAggregateCSVFile( dbname, entbl, uri, worklayer ):
#
#   dbname   作業用データベースファイル名
#   entbl   調査結果格納テーブル名
#   uri     入力CSVファイル (UTF-8でなければいけない）
#   worklayer    集計結果格納テーブル名


def  ImportandAggregateCSVFile( dbname, entbl, uri, worklayer ):


     conn = sqlite3.connect(dbname)
     # sqliteを操作するカーソルオブジェクトを作成
     cur = conn.cursor()


     # 調査結果格納テーブルの作成
     crsql = 'CREATE TABLE \"' + entbl + '\"( address STRING, vn  STRING);'
     cur.execute( crsql)


     # csv file read   UTF-8 限定
     with open( uri, 'r', encoding="utf_8" ) as f:
          b = csv.reader(f)
          header = next(b)
     
          isql = 'insert into \"' + entbl + '\" values (?,?);'
          for t in b:
 
              cur.execute(isql, t)

# データベースへコミット。これで変更が反映される。
     conn.commit()


     sqlstr = 'create table "' + worklayer + '" as select address, count(*) from \"' + entbl + '\"  group by address;'
# 町名別集計
     cur.execute(sqlstr )

     conn.close()




# Intersect ポリゴンと元の行政界ポリゴンの面積比とサンプル数値をかけてInterSectポリゴン単位の案分サンプル値を作成する
#  defCalcDataUsingRatio(  outputdb, intersect_output, area_column,ratio_column , out_table, ad_areacolumn)
#
#   outputdb   ワークDBファイル
#   intersect_output    Intersect 結果
#   area_column   面積出力カラム名
#   ratio_column   按分集計値出力カラム名
#   out_table     出力テーブル名
#  ad_areacolumn        行政界ポリゴンテーブルの面積値格納カラム名
#

def CalcDataUsingRatio(  outputdb, intersect_output, area_column, ratio_column , out_table , ad_areacolumn ):
#  Intersect 結果に対する 面積算出

     feedback = QgsProcessingFeedback()
     
     intersect_name = outputdb + "|layername=" + intersect_output 

     ilayer = QgsVectorLayer(intersect_name, "intersect", "ogr")

     if ilayer.isValid():
              print("intersect Layer load OK")
     else:
              print("intersect Layer load Fail")
              sys.exit()

     ilayer.setProviderEncoding('utf-8')




     hogehogehoge_id = str(uuid.uuid4())

     output_tbl3 = 'ogr:dbname=\'' + outputdb + '\' table=\"intersect' +  hogehogehoge_id +  '\" (geom) sql='


     params3 = { 'INPUT' : ilayer, 'FIELD_NAME' : area_column , 'FIELD_TYPE': 0, 'FIELD_LENGTH':12, 'FIELD_PRECISION':5, 'NEW_FIELD':1,'FORMULA':'$area','OUTPUT' : output_tbl3 }

     res = processing.run('qgis:fieldcalculator', params3, feedback=feedback)


     #print( output_tbl3 )
#  



#  行政界ポリゴンと交差ポリゴンの面積比を 集計値にかけて交差ポリゴン単位の集計値を算出する

     intersect2_name =  outputdb + "|layername=" + 'intersect' +  hogehogehoge_id

     i2layer = QgsVectorLayer(intersect2_name, "intersect2", "ogr")

     if i2layer.isValid():
              print("intersect Layer2 load OK")
     else:
              print("intersect Laye2r load Fail")
              sys.exit()

     i2layer.setProviderEncoding('utf-8')


     output_tbl4 = 'ogr:dbname=\'' + outputdb + '\' table=\"' + out_table  +  '\" (geom) sql='


     expstr = '\"' + area_column + '\"/ \"'  + ad_areacolumn + '\" * \"count(*)\"'

     params4 = { 'INPUT' : i2layer, 'FIELD_NAME' : ratio_column, 'FIELD_TYPE': 0, 'FIELD_LENGTH':12, 'FIELD_PRECISION':5, 'NEW_FIELD':1,'FORMULA':expstr ,'OUTPUT' : output_tbl4 }

     res = processing.run('qgis:fieldcalculator', params4, feedback=feedback)



     #print( output_tbl4 )

     return( output_tbl4 )
     
     



#
#   work_db       出力DBファイル
#    input_table    入力テーブル名
#   sample_column   サンプル数値カラム名

#   output_table    出力テーブル名

#   output_column   出力数値カラム名
#   meshid_column   メッシュIDカラム名
# 交差ポリゴン単位の集計値をメッシュ単位に再集計する
def  RecalcUsingMesh(  work_db, input_table , sample_column, output_table, output_colmn ,meshid_column):

     conn = sqlite3.connect(work_db)
     #print( outputdb )
     cur = conn.cursor()

# 調査結果格納テーブルの作成


     sumsql = 'create table \"'+ output_table + '\" as  select ' + meshid_column + ', cast( sum(' + sample_column + ') as float) as ' + output_colmn + '  from \"' +  input_table + '\" group by ' + meshid_column

     print( sumsql )
     cur.execute( sumsql)



     gpkgcstr = 'insert into gpkg_contents (table_name,data_type,identifier,srs_id) values(\"' + output_table + '\", \"attributes\", \"' + output_table + '\", 0 )'
     print( gpkgcstr )
     cur.execute( gpkgcstr )

     conn.commit()


     print( output_table )
     
#
#   メッシュと集計ファイルの結合
#     
#   work_db       出力DBファイル
#   mlayer      メッシュレイヤ
#   keycolum      キーカラム名
#   stattable     集計テーブル名
#   keycolum2     集計テーブルキーカラム名
#   output_table    出力テーブル名

def  ConnectMeshandStat( meshlayer, workdb,  keycolum, stattable, keycolum2, output_table ):
#mlayer
     meshstat_name =  workdb + "|layername=" + stattable

     mstatlayer  = QgsVectorLayer(meshstat_name , "meshstat", "ogr")
     feedback = QgsProcessingFeedback()
     
     if mstatlayer.isValid():
              print("mstat layer load OK")
     else:
              print("mstat layer load Fail")
              sys.exit()
         

     output_tbl6 = 'ogr:dbname=\'' + workdb + '\' table=\"' +  output_table +  '\" (geom) sql='

     print(output_tbl6)

     params6 = { 'DISCARD_NONMATCHING' : False, 'FIELD' : keycolum, 'FIELDS_TO_COPY' : [], 'FIELD_2' : keycolum2, 'INPUT' : meshlayer,
           'INPUT_2' : mstatlayer , 'METHOD' : 1, 'OUTPUT' : output_tbl6, 'PREFIX' : '' }

     res = processing.run('native:joinattributestable', params6, feedback=feedback)


#  outputdb   DB ファイル
#  output_tbl メッシュテーブル
#  sample_column  サンプル数カラム
#  limit_n    最小サンプル数
#
#  return  最小サンプル数以下のメッシュの数
#  
#  最小サンプル数以下のメッシュの数の判定
def CheckMinimumSample( outputdb, output_tbl, sample_column, limit_n ,divcolumn ):

     conn = sqlite3.connect(outputdb)
     conn.enable_load_extension(True)
     #conn.execute("select load_extension('mod_spatialite')") 
     cur = conn.cursor()



     countsql = 'select count(*) from \"' + output_tbl + '\" where ' + sample_column +  ' > 0 and ' + sample_column + ' < ' + str(limit_n) + ' and ' + sample_column + ' IS NOT NULL and '+ divcolumn + ' <> 1'

     print( countsql )


     res = cur.execute( countsql )

     nc =  cur.fetchone()[0]
     conn.commit()
     
     return( nc )

#
#  分割不能メッシュを親メッシュで置き換えする
#
#  outputdb   DB ファイル
#  meshtable メッシュテーブル   
#  inputAdmstat   行政界別集計レイヤ Polygon + 集計値
#
#  output_table  出力メッシュテーブル  
#  SplitFlag     不均等分割フラグ  1 の場合不均等分割を行う
#  recLevel    再帰レベル  初期値は 1
#  limit_sample   最小サンプル数
#
def  ModifyUndividePolygon(  outputdb, output_tbl, sample_column , limit_n, keycolumn, divcolumn, inputMesh ):
     conn = sqlite3.connect(outputdb)
     conn.enable_load_extension(True)
     conn.execute("select load_extension('mod_spatialite')") 
     cur = conn.cursor()
     countsql = 'select '+ keycolumn+ ',' + sample_column + ' from \"' + output_tbl + '\" where ' + sample_column +  ' > 0 and ' + sample_column + ' < ' + str(limit_n) + ' and ' + sample_column + ' IS NOT NULL and '+ divcolumn + ' <> 1'
 
     
     dicE = {} 
     
     for row in  cur.execute( countsql ):
     
          dicE[row[0][:-3]] = row[0]
          
          #print( key )
     
     
     #print( dicE )
     
     #     不要メッシュの削除
     for k, v in dicE.items(): 
    #      print( k ,v )   
          
          delsql = 'delete from \"' + output_tbl + '\" where ' + keycolumn + ' like \'' + k + '___\''
   #  delsql = 'delete from \"' + output_tbl + '\" where ' + sample_column +  ' > 0 and ' + sample_column + ' < ' + str(limit_n) + ' and ' + sample_column + ' IS NOT NULL and '+ divcolumn + ' <> 1'
   #       print( delsql )
          cur.execute( delsql )
     
     for k, v in dicE.items(): 
          qsql = 'select * from \"' + inputMesh + '\" where ' + keycolumn + '=\'' + k + '\''
          print( qsql )
          cur.execute( qsql )
          nc =  cur.fetchone()
          #print( nc )
          
          isql = 'insert into \"' + output_tbl + '\" ( geom, code, divide_end, snum )  values ( ?,?,?,?)'
          
          prm = ( nc[1], nc[2], 1, nc[5] )
          cur.execute( isql, prm )
          
     conn.commit()


#
#  分割不能メッシュを親メッシュで置き換えする   GeoPackage 版
#
#  outputdb   DB ファイル
#  meshtable メッシュテーブル   
#  inputAdmstat   行政界別集計レイヤ Polygon + 集計値
#
#  output_table  出力メッシュテーブル  
#  SplitFlag     不均等分割フラグ  1 の場合不均等分割を行う
#  recLevel    再帰レベル  初期値は 1
#  limit_sample   最小サンプル数
#
def  ModifyUndividePolygonGeop(  outputdb, output_tbl, sample_column , limit_n, keycolumn, divcolumn, inputMesh ):
     conn = sqlite3.connect(outputdb)
     conn.enable_load_extension(True)
     #conn.execute("select load_extension('mod_spatialite')") 
     cur = conn.cursor()
     countsql = 'select '+ keycolumn+ ',' + sample_column + ' from \"' + output_tbl + '\" where ' + sample_column +  ' > 0 and ' + sample_column + ' < ' + str(limit_n) + ' and ' + sample_column + ' IS NOT NULL and '+ divcolumn + ' <> 1'
 
     
     dicE = {} 
     
     for row in  cur.execute( countsql ):
     
          dicE[row[0][:-3]] = row[0]
          
          #print( key )
     
     
     #print( dicE )
     
     #     不要メッシュの削除
     for k, v in dicE.items(): 
    #      print( k ,v )   
          
          delsql = 'delete from \"' + output_tbl + '\" where ' + keycolumn + ' like \'' + k + '___\''
   #  delsql = 'delete from \"' + output_tbl + '\" where ' + sample_column +  ' > 0 and ' + sample_column + ' < ' + str(limit_n) + ' and ' + sample_column + ' IS NOT NULL and '+ divcolumn + ' <> 1'
   #       print( delsql )
          cur.execute( delsql )
     
     for k, v in dicE.items(): 
          qsql = 'select * from \"' + inputMesh + '\" where ' + keycolumn + '=\'' + k + '\''
          print( qsql )
          cur.execute( qsql )
          nc =  cur.fetchone()
          #print( nc )
          
          isql = 'insert into \"' + output_tbl + '\" ( geom, code, divide_end, snum )  values ( ?,?,?,?)'
          
          prm = ( nc[1], nc[2], 1, nc[5] )
          cur.execute( isql, prm )
          
     conn.commit()     


# 分割用初期メッシュテーブルの作成    不均等分割のために分割作業終了フラグをカラムにいれる
#     分割終了フラグ    カラム名  dvide_end    0   終了していない    1   終了している
#  outputdb   DB ファイル
#  output_tbl メッシュテーブル    sqlite3.OperationError: no such function ST_IsEmpty  がでるから改修はあとで

def  AddDivideFlag(  outputdb, output_tbl ):
      conn = sqlite3.connect(outputdb)
     #print( outputdb )
      cur = conn.cursor()


      sumsql = 'alter table \"'+ output_tbl + '\" add column divide_end integer default 0'

      print( sumsql )
      cur.execute( sumsql)


      #sumsql2 = 'update  \"'+ output_tbl + '\" set divide_end=0'
      #print( sumsql2 )
      #cur.execute( sumsql2 )

      conn.commit()


#
#    2点の中点
def  GetCyuuten( p1, p2 ):

     x1 = p1.x()
     y1 = p1.y()
     
     x2 = p2.x()
     y2 = p2.y()
     
     
     return (GetCyuutenXY( x1, y1, x2, y2 ))

#
#    2点の中点
#    
def  GetCyuutenXY( x1, y1, x2, y2 ):
     
     #print( x1 )
     #print( y1 )
     
     xm = ( x1 + x2 ) /2.0
     ym = ( y1 + y2 ) /2.0
     
     
     return QgsPointXY( xm, ym )



#
#  メッシュ分割
#
#  outputdb   DB ファイル
#  meshtable メッシュテーブル   
#  output_table  出力メッシュテーブル  
#  SplitFlag     フラグが1のメッシュは分割しない  

def SplitMesh( outputdb, meshtable,  output_table , SplitFlag, keycolum,  divcolumn  ):


     mlayer = meshtable
     
     if  type(meshtable) is str: 
               
     #    入力メッシュレイヤ
          dmesh = outputdb + "|layername=" + meshtable


          mlayer = QgsVectorLayer(dmesh, "mesh", "ogr")

          

     if mlayer.isValid():
              print("dmesh Layer load OK")
     else:
              print("dmesh Layer load Fail")
              print( "dmesh=" + meshtable );
              sys.exit()
 
     out_tb = outputdb + "|layername=" + output_table 
 
 
     crsstr = mlayer.crs().authid()
 
 
     #   作業結果出力レイヤ
     
     vectL = 'Polygon?crs=' + crsstr
     
     vl1 = QgsVectorLayer( vectL , "temporary_mesh", "memory")
                 
                 
     if not vl1:
              print("Virtual Layer failed to load!")  
              sys.exit()  
     else:
              print( out_tb )
              
     #vl1.setCrs( mlayer.crs()  )
                         
     pr1 = vl1.dataProvider()
                      
            #  フィールド定義
     pr1.addAttributes([
                    QgsField(keycolum, QVariant.String),
                    QgsField(divcolumn,  QVariant.Int)])
     
             
     vl1.updateFields() # 
     
     vl1.beginEditCommand("Add Polygons")
                    
     features = mlayer.getFeatures()


     for feature in features:


         code = feature[ keycolum]
         divide_f = feature[ divcolumn ]
         
         
         #print( 'code =' + code+ ' divide=' + str(divide_f) )
         geom = feature.geometry()
         geomSingleType = QgsWkbTypes.isSingleType(geom.wkbType())
         
         if divide_f == 0:
         
              if geom.type() == QgsWkbTypes.PolygonGeometry:
         
                  if geomSingleType:
                        x = geom.asPolygon()
              

                   #print("Polygon: ", x, "Area: ", geom.area())
                   
                   
                        for xp in x:
                         #print("xp:",xp )
                         
                         #for xxp in xp:
                         #     print("xxp:",xxp)
                       
                         #    座標の場所を判定して位置関係を正規化したほうがいいかも
                         #
                         
                              p0_1 =  GetCyuuten( xp[0], xp[1] )
                              p1_2 =  GetCyuuten( xp[1], xp[2] )       
                              p2_3 =  GetCyuuten( xp[2], xp[3] )    
                              p3_4 =  GetCyuuten( xp[3], xp[4] ) 
                              pC_C =  GetCyuuten( p0_1, p2_3 )                          
                              

                         
                        
                     
                         #    新しいキーコード
                              ncode1 = code + '-01'
                         
                              Polygon1 = QgsGeometry.fromPolygonXY([[QgsPointXY(xp[0].x(), xp[0].y()),
                                         QgsPointXY(p0_1.x(), p0_1.y()), QgsPointXY(pC_C.x(), pC_C.y()), QgsPointXY(p3_4.x(), p3_4.y())]])
                                         
                                         
                          # add a feature
                              fet = QgsFeature(pr1.fields())
                          
                              fet.setGeometry(Polygon1)
                                        
                                                                        
                              fet[keycolum] =ncode1
                              fet[divcolumn] = divide_f 
                         #  新しい feature を作って別レイヤに格納する
                              retc = pr1.addFeatures([fet])
                            
                         #print("add new")
                         #print( retc )      
                                
                        #    新しいキーコード
                              ncode2 = code + '-02'       
                              Polygon2 = QgsGeometry.fromPolygonXY([[
                                         QgsPointXY(p0_1.x(), p0_1.y()), QgsPointXY(xp[1].x(), xp[1].y()), QgsPointXY(p1_2.x(), p1_2.y()), QgsPointXY(pC_C.x(), pC_C.y())]])     
                                         
                                         
                         
                              fet2 = QgsFeature(pr1.fields())
                          
                              fet2.setGeometry(Polygon2)
                                        
                                                                        
                              fet2[keycolum] =ncode2
                              fet2[divcolumn] = divide_f 
                         #  新しい feature を作って別レイヤに格納する
                              retc = pr1.addFeatures([fet2])
                            
                                 
                        #    新しいキーコード
                              ncode3 = code + '-03'       
                              Polygon3 = QgsGeometry.fromPolygonXY([[
                                         QgsPointXY(pC_C.x(), pC_C.y()), QgsPointXY(p1_2.x(), p1_2.y()), QgsPointXY(xp[2].x(), xp[2].y()), QgsPointXY(p2_3.x(), p2_3.y())]])     
                                         
                                         
                         
                              fet3 = QgsFeature(pr1.fields())
                          
                              fet3.setGeometry(Polygon3)
                                        
                                                                        
                              fet3[keycolum] =ncode3
                              fet3[divcolumn] = divide_f 
                         #  新しい feature を作って別レイヤに格納する
                              retc = pr1.addFeatures([fet3])                        
                         
  
  
  
                        #    新しいキーコード
                              ncode4 = code + '-04'       
                              Polygon4 = QgsGeometry.fromPolygonXY([[
                                         QgsPointXY(p3_4.x(), p3_4.y()), QgsPointXY(pC_C.x(), pC_C.y()), QgsPointXY(p2_3.x(), p2_3.y()) , QgsPointXY(xp[3].x(), xp[3].y())]])     
                                         
                                         
                         
                              fet4 = QgsFeature(pr1.fields())
                          
                              fet4.setGeometry(Polygon4)
                                        
                                                                        
                              fet4[keycolum] =ncode4
                              fet4[divcolumn] = divide_f 
                         #  新しい feature を作って別レイヤに格納する
                              retc = pr1.addFeatures([fet4])                  
                         
                         
                                                                

                         #print(Polygon1)
                        # print(Polygon2)
                         
                         
                         #Poly

                         #feat.setGeometry( QgsGeometry.fromPolygonXY([QgsPointXY(546016, 4760165), p2, p3]))
                         #qPolygon1 = QgsGeometry.fromPolygonXY([ xp[0],p0_1, pC_C,xp[0]])
                         
                         #print( qPolygon1 )
                              
                  #   一点目 2点目の中点を求める   2
                  
                  else:       
                        x = geom.asMultiPolygon()
                   #print("MultiPolygon: ", x, "Area: ", geom.area())
              else:
                   print("geometry is not polygon!")
                   
         else:    #  分割不要ポリゴンはそのまま書き込む
              retc = pr1.addFeatures([feature])              
     
     vl1.updateExtents()     
     vl1.endEditCommand()
     vl1.commitChanges()       
     
     
     features2 = vl1.getFeatures()
     
     print("-------------------------- vl1 features ------------------------")
     #for feature2 in features2:
     
     #          print( feature2 )
               
     options = QgsVectorFileWriter.SaveVectorOptions()
     options.driverName = 'GPKG'
     options.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer 
     options.layerName = output_table 
    
    
     #print( "outputdb ==>"+outputdb )
     #print( "output ==>"+output_table )
     
     #  結果レイヤ書き込み
     write_result, error = QgsVectorFileWriter.writeAsVectorFormat(vl1,outputdb ,options)
     #if error == QgsVectorFileWriter.NoError:
     #         print("success again!")
     
     #print( error )
     #print( write_result )
     
     return ( output_table )
     
     #   出力結果テーブルを返す
     

#
#  メッシュ分割と分割メッシュでの再集計 (再帰呼出しを想定)
#
#  outputdb   DB ファイル
#  meshtable メッシュテーブル   
#  inputAdmstat   行政界別集計レイヤ Polygon + 集計値
#
#  output_table  出力メッシュテーブル  
#  SplitFlag     不均等分割フラグ  1 の場合不均等分割を行う
#  recLevel    再帰レベル  初期値は 1
#  limit_sample   最小サンプル数

def RemeshData( outputdb, inputMesh, inputAdm, limit_sample, maxlevel, SplitFlag, keycolum,  divcolumn , recLevel):

     #   メッシュ分割

     hogehoge_id = str(uuid.uuid4())
     
     output_tbl = "rmesh" + hogehoge_id
     
     cmesh = SplitMesh( outputdb, inputMesh ,output_tbl , SplitFlag, keycolum, divcolumn )
     
     
     #  作成メッシュで再集計
     
     #  Inter sect
        #  出力テーブル名     
 
     intersect_output = "intersect" + hogehoge_id


     output_tbl2 = 'ogr:dbname=\'' + outputdb + '\' table=\"' + intersect_output +  '\" (geom) sql='
     
     mlayername = outputdb + "|layername=" + cmesh
     
     mlayer = QgsVectorLayer(mlayername, "mesh", "ogr")

     if mlayer.isValid():
              print("mesh Layer load OK")
     else:
              print("mesh Layer load Fail")
              sys.exit()


     ExecuteInterSect( mlayer, inputAdm, output_tbl2  )
     
     
     
     hoge_id4 = str(uuid.uuid4())
     out_table4 = 'intersect' + hoge_id4


     # Intersect ポリゴンと元の行政界ポリゴンの面積比とサンプル数値をかけてInterSectポリゴン単位の案分サンプル値を作成する
     CalcDataUsingRatio(  outputdb, intersect_output, 's_area','countmratio' , out_table4, 'parea')


     input_table = out_table4
     sample_column = 'countmratio'
     output_table ="meshsum" + hoge_id4
     output_colmn = 'snum'
     meshid_column = 'code'



     # 交差ポリゴン単位の集計値をメッシュ単位に再集計する
     RecalcUsingMesh(  outputdb, input_table , sample_column, output_table, output_colmn, meshid_column  )
     
     
     #   メッシュと集計ファイルの結合
     keycolum = 'code'
     keycolum2 = 'code'
     stattable = output_table
     output_tbl = 'mesh' +  hoge_id4 
     ConnectMeshandStat( mlayer, outputdb,  keycolum, stattable, keycolum2, output_tbl )
     
     #  サンプル値チェック
     output_colmn = 'snum'
     
     #  最小サンプル数以下のメッシュの数の判定
     mc = CheckMinimumSample( outputdb, output_tbl, output_colmn , limit_sample, divcolumn )

       #   不均等分割の場合を後で実装する
     if mc > 0 :
            if int(SplitFlag) > 0 :
                 print("mc =>" + str(mc) + " divide flag=>" + str(SplitFlag ))
                 
                 #ModifyUndividePolygonGeop(  outputdb, output_tbl, output_colmn , limit_sample, keycolum, divcolumn, inputMesh )

                 ModifyUndividePolygon(  outputdb, output_tbl, output_colmn , limit_sample, keycolum, divcolumn, inputMesh )
                 #return( inputMesh )

            
            
            else:
                 print("mc =>" + str(mc) + "divide flag=>" + str(SplitFlag ))
                 return( inputMesh )

     
     #  再帰レベルチェック
     
     nRec = recLevel + 1
     
     if maxlevel > 0:
          if nRec > maxlevel:
               return( inputMesh )
     
     print( "max =>" + str(maxlevel) + "  now ->"+ str(nRec) + " mc=>" + str(mc ))
     
     #  再度コール
     return( RemeshData( outputdb, output_tbl, inputAdm, limit_sample, maxlevel, SplitFlag, keycolum,  divcolumn , nRec ))


