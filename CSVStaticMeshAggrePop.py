# -*- coding: utf-8 -*-

"""
/***************************************************************************
 CSVStaticMeshAggrePop
                                 A QGIS plugin
 Read a CSV file and calculate the number of lines for each item in the specified column
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                              -------------------
        begin                : 2020-05-15
        copyright            : (C) 2020 by Yoichi Kayama
        email                : yoichi.kayama@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

__author__ = 'Yoichi Kayama'
__date__ = '2020-12-11'
__copyright__ = '(C) 2020 by Yoichi Kayama'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

from qgis.PyQt.QtCore import ( QCoreApplication ,
                           QVariant)

from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterEnum,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterVectorDestination,
                       QgsProcessingParameterFile,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterBoolean,
                       QgsProcessingParameterField,
                       QgsProcessingParameterNumber,
                       QgsProcessingOutputVectorLayer,
                       QgsVirtualLayerDefinition,
                       QgsVectorLayer,
                       QgsProcessingUtils,
                       QgsProcessingMultiStepFeedback,
                       QgsWkbTypes,
                       QgsField,
                       QgsFields,
                       QgsFeature,
                       QgsProcessingParameterString)


import processing
import sqlite3
import csv

from .mod import  agtools
#SplitMeshLayer( last_output ,  meshid  )

class CSVStatMeshAggrePopProcessingAlgorithm(QgsProcessingAlgorithm):
    """
    This is an example algorithm that takes a vector layer and
    creates a new identical one.

    It is meant to be used as an example of how to create your own
    algorithms and explain methods and variables used to do it. An
    algorithm like this will be available in all elements, and there
    is not need for additional work.

    All Processing algorithms should extend the QgsProcessingAlgorithm
    class.
    """

    # Constants used to refer to parameters and outputs. They will be
    # used when calling the algorithm from another algorithm, or when
    # calling from the QGIS console.

    OUTPUT = 'OUTPUT'
    INPUT = 'INPUT'

    #ENCODING = 'ENCODING'

    #encode = ['SJIS','UTF-8']

    proportional_div = ['人口','面積']


    def initAlgorithm(self, config):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """
        self.addParameter(
            QgsProcessingParameterFile(
                self.INPUT,
                'CSVファイル',
                extension='csv'
            )
         )
        #  encoding of input file
        #encParam = QgsProcessingParameterEnum(
        #        self.ENCODING,
        #        'CSVファイルエンコーディング'
        #    )

        #encParam.setOptions(self.encode)
        #encParam.setAllowMultiple(False)
        #encParam.setDefaultValue(QVariant('SJIS'))
        #  file encoding
        #self.addParameter(
        #    encParam
        #)
        self.addParameter(QgsProcessingParameterEnum('ENCODING', 'CSVファイルエンコーディング', options=['SJIS','UTF-8'], allowMultiple=False, defaultValue='SJIS'))



        self.addParameter(QgsProcessingParameterVectorLayer('addresslayer', '住所レイヤ',
                         types=[QgsProcessing.TypeVectorPolygon], defaultValue=None))
        self.addParameter(QgsProcessingParameterField('addressfield', '住所フィールド', 
                         type=QgsProcessingParameterField.String, parentLayerParameterName='addresslayer', allowMultiple=False, defaultValue=None))

        self.addParameter(QgsProcessingParameterVectorLayer('meshlayer', 'メッシュレイヤ',
                         types=[QgsProcessing.TypeVectorPolygon], optional=False, defaultValue=None))
        
        self.addParameter(QgsProcessingParameterField('meshid', 'メッシュIDフィールド', 
                         type=QgsProcessingParameterField.String, parentLayerParameterName='meshlayer', optional=False, allowMultiple=False))


        self.addParameter(QgsProcessingParameterNumber('limit_sample', '最小サンプル数',
                          defaultValue=3))

        self.addParameter(QgsProcessingParameterNumber('maxdivide', '最大分割回数',
                          defaultValue=4))

        self.addParameter(QgsProcessingParameterBoolean('uneven_div', '不均等分割',
                          defaultValue=False))


        self.addParameter(QgsProcessingParameterVectorLayer('popmeshlayer', '人口メッシュレイヤ',
                         types=[QgsProcessing.TypeVectorPolygon], optional=False, defaultValue=None))
        
        self.addParameter(QgsProcessingParameterField('popmeshid', '人口メッシュIDフィールド', 
                         type=QgsProcessingParameterField.String, parentLayerParameterName='meshlayer', optional=False, allowMultiple=False))


        self.addParameter(QgsProcessingParameterField('popmeshpop', '人口メッシュ人口フィールド', 
                         type=QgsProcessingParameterField.Numeric, parentLayerParameterName='meshlayer', optional=False, allowMultiple=False))



        #  propotinal division method
        #propParam = QgsProcessingParameterEnum(
        #        "PROPDIV",
        #        self.tr('按分方法選択')
        #    )

        #propParam.setOptions(self.proportional_div)
        #propParam.setAllowMultiple(False)
        #propParam.setDefaultValue(QVariant('人口'))
        #  file encoding
        #self.addParameter(
         #   propParam
        #)

        #self.addParameter(QgsProcessingParameterVectorLayer('poplayer', '人口レイヤ',
        #                 types=[QgsProcessing.TypeVectorPolygon], optional=True,  defaultValue=None))
        #self.addParameter(QgsProcessingParameterField('popfield', '人口フィールド', 
        #                 type=QgsProcessingParameterField.String, parentLayerParameterName='addresslayer', optional=True, allowMultiple=False, defaultValue=None))
        


        # We add a feature sink in which to store our processed features (this
        # usually takes the form of a newly created vector layer when the
        # algorithm is run in QGIS).
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT,
                self.tr('Output layer')
            )
        )

    def processAlgorithm(self, parameters, context, model_feedback):
        """
        Here is where the processing itself takes place.
        """
        results = {}

        feedback = QgsProcessingMultiStepFeedback(1, model_feedback)

        csvfile = self.parameterAsFile(
            parameters,
            self.INPUT,
            context
        )
        if csvfile  is None:
            raise QgsProcessingException(self.tr('csv file error'))

        #df = QgsVirtualLayerDefinition()

        enc = self.parameterAsInt(           
            parameters,
            'ENCODING',
            context)

        #enc = self.parameterAsFile(
        #    parameters,
        #    self.ENCODING,
        #    context
        #)
        meshLayer = self.parameterAsVectorLayer(
            parameters,
            "meshlayer",
            context
        )
        if meshLayer  is None:
            raise QgsProcessingException(self.tr('mesh layer missed'))

        meshidfields = self.parameterAsFields  (
             parameters,
             'meshid',
             context
        )


        limit_sample = self.parameterAsInt ( parameters,
             'limit_sample',
             context)

        maxdivide = self.parameterAsInt ( parameters,
             'maxdivide',
             context)

 
        uneven_div = self.parameterAsInt ( parameters,
             'uneven_div',
             context)


        popmeshLayer = self.parameterAsVectorLayer(
            parameters,
            "popmeshlayer",
            context
        )
        if meshLayer  is None:
            raise QgsProcessingException(self.tr('mesh layer missed'))

        popmeshidfields = self.parameterAsFields  (
             parameters,
             'popmeshid',
             context
        )

                   


        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # 住所別集計
        alg_params = {
        
            'addresslayer': parameters['addresslayer'],
            'addressfield': parameters['addressfield'],
            'INPUT': csvfile,
            'ENCODING': enc,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        #Stat_CSVAddressPolygon

        outputs_statv = processing.run('QGIS_stat:Stat_CSVAddressPolygon', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        if feedback.isCanceled():
            return {}

        statv = outputs_statv["OUTPUT"]
        meshid = meshidfields[0]




        #   人口メッシュと行政界メッシュのUnion作成する

        param_uni  = { 'INPUT' : statv, 
                 'OUTPUT' : QgsProcessing.TEMPORARY_OUTPUT, 'aggrefield' : 'count', 
                 'meshid' : meshid,
                'meshlayer' : meshLayer}




        param1 = { 'INPUT' : statv, 
                 'OUTPUT' : QgsProcessing.TEMPORARY_OUTPUT, 'aggrefield' : 'count', 
                 'meshid' : meshid,
                'meshlayer' : meshLayer}
        



        #parameters['OUTPUT']
        #     
        res1 = processing.run('QGIS_stat:AggregateAdmbyMeshAlgorithm', param1, context=context, feedback=feedback, is_child_algorithm=True)

        if feedback.isCanceled():
            return {}


        numberof_under_limit = 0

        #numberof_under_limit = res1["LIMITPOL"]



                        # レイヤをGeoPackage化
        alg_paramsg = {
                           'LAYERS': res1["OUTPUT"],
                           'OVERWRITE': True,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
        retg1 = processing.run('native:package', alg_paramsg, context=context, feedback=feedback, is_child_algorithm=True)
        last_output = retg1["OUTPUT"]

        new_mesh = retg1["OUTPUT"]

        mesh_layb = retg1["OUTPUT"]

        if type(mesh_layb) is str:
            mesh_layb =    QgsVectorLayer(mesh_layb, "mesh", "ogr")

        numberof_under_limit = 0

        #    作業用レイヤの作成
        crs_str = mesh_layb.crs()

        layerURI = "Polygon?crs=" + crs_str.authid()
        #feedback.pushConsoleInfo( "work layer  " + layerURI  )
        resLayer = QgsVectorLayer(layerURI, "mesh_result", "memory")

        appended = {}

        adfields = []
        for field in mesh_layb.fields():
            #print(field.name(), field.typeName())
            adfields.append(field)
            #resLayer.addField(field)

        resLayer.dataProvider().addAttributes(adfields)
        resLayer.updateFields()


        lower_ids = []

        value_column = "value"

        #    limit 値より小さい値のポリゴン数算出
        for f in  mesh_layb.getFeatures():
            # feedback.pushConsoleInfo( "value  " +str( f["value"])  )
            if not f[value_column] is None:
                if f[value_column] > 0 and f[value_column] <  limit_sample :
                    numberof_under_limit += 1
                    lower_ids.append( f[meshid])


        next_output = None


                    #   集計結果が最小サンプルより小さいものがある場合
        if numberof_under_limit > 0:
                   #  均等分割の場合は終了
            if uneven_div:


                rmid = []
                for tgid in ( lower_ids):
                    feedback.pushConsoleInfo( "lower id  " +str( tgid )  )

                           #  next_output   code   の下3桁　削除   C27210-02    -> C27210   が last_output の code 番号
                           #  next_output  では last_output  が同じ番号の最大4メッシュを削除する

                           # リミットより小さいレコードは旧レコードを退避
                           #  リミットにひっかかるレコードを再処理用リストから削除（同一親メッシュのものも削除）
                       #   不均等分割でリミット以下のデータがある場合は last_output -> 分割不能抽出   next_output  分割不能削除  next_output -> last_output 代入
                    parent_code = tgid[0:-3]

                           
                    rmid.append(parent_code)

                addfeatures = []

 


                alg_paramsg_n = {
                           'LAYERS': last_output,
                           'OVERWRITE': False,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
                lmesh  = processing.run('native:package', alg_paramsg_n, context=context, feedback=feedback, is_child_algorithm=True)  


                last_output = lmesh["OUTPUT"]

                if type(last_output) is str:
                    last_output =  QgsVectorLayer(last_output, "mesh", "ogr")

                last_output.selectAll()


                for lf in  last_output.getFeatures():

                    for pcode in ( rmid ):
                        #    feedback.pushConsoleInfo( "pcode  " + pcode+ " meshid =" + lf[meshid]  )
                        if lf[meshid]== pcode:
                            lf["fid"] = None
                            if not lf[value_column] :
                                lf[value_column]  = 0.0

                            if lf[meshid]  not in appended :

                                addfeatures.append(lf)
                                appended[lf[meshid]]  = lf
                                 
                     #       feedback.pushConsoleInfo( "add feature   " + pcode  )


                resLayer.dataProvider().addFeatures( addfeatures)



                deleteFeatures = []

                if type(next_output) is str:
                    next_output =  QgsVectorLayer(next_output, "mesh", "ogr")

                for nf in  next_output.getFeatures():

                    for pcode in ( rmid ):
                        if nf[meshid][0:-3]== pcode:
                            deleteFeatures.append(nf.id())
                            feedback.pushConsoleInfo( "delete id  " +str( pcode )  )
                                    
                next_output.dataProvider().deleteFeatures(deleteFeatures)


                last_output = next_output


         #  分割回数ループ
        for divide_c in range(1,maxdivide ):

            if numberof_under_limit > 0:
                   #  均等分割の場合は終了
                if not uneven_div:
                    break
#------------------------------------------------------------------------------------------------------------------------

            #  最小サンプルより小さいものが無い場合はメッシュ分割
            #else:


            if type( last_output ) is str:
                feedback.pushConsoleInfo( "last output " + last_output  )   
            else:
                 feedback.pushConsoleInfo( "last output " + last_output.name()  )   

            alg_paramsg_m = {
                           'LAYERS': last_output,
                           'OVERWRITE': True,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
            spmesh  = processing.run('native:package', alg_paramsg_m, context=context, feedback=feedback, is_child_algorithm=True)                               

            new_mesh = agtools.SplitMeshLayer( spmesh["OUTPUT"] ,  meshid  )

            # statv  行政界別集計データ
             
            #  再度メッシュ集計
            param2 = { 'INPUT' : statv, 
                        'OUTPUT' : QgsProcessing.TEMPORARY_OUTPUT, 'aggrefield' : 'count', 
                         'meshid' : meshid,
                        'meshlayer' : new_mesh}

            res2 = processing.run('QGIS_stat:AggregateAdmbyMeshAlgorithm', param2, context=context, feedback=feedback, is_child_algorithm=True)

                   #numberof_under_limit = res2["LIMITPOL"]
            numberof_under_limit = 0
                  # レイヤをGeoPackage化
            alg_paramsg2 = {
                           'LAYERS': res2["OUTPUT"],
                           'OVERWRITE': True,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
            retg2 = processing.run('native:package', alg_paramsg2, context=context, feedback=feedback, is_child_algorithm=True)

            mesh_layb = retg2["OUTPUT"]

            if type(mesh_layb) is str:
                mesh_layb =    QgsVectorLayer(mesh_layb, "mesh", "ogr")

      
                  
                   #features = mesh_layb.selectedFeatures()
                   #feedback.pushConsoleInfo( "feature count  " +str( len(features))  )
            lower_ids = []
            for f in  mesh_layb.getFeatures():
                    #   feedback.pushConsoleInfo( "value  " +str( f["value"])  )
                if not f[value_column] is None:
                    if f[value_column] > 0 and f[value_column] <  limit_sample :
                        numberof_under_limit += 1
                        lower_ids.append( f[meshid])




            if numberof_under_limit == 0:
                last_output = res2["OUTPUT"]
                next_output = retg2["OUTPUT"]
            else:
                       #   不均等分割でリミット以下のデータがある場合は last_output -> 分割不能抽出   next_output  分割不能削除  next_output -> last_output 代入
               # last_output = res2["OUTPUT"]
                next_output = retg2["OUTPUT"]



            #   集計結果が最小サンプルより小さいものがある場合
            if numberof_under_limit > 0:
                   #  均等分割の場合は終了
                if not uneven_div:

                    break

                   #  不均等分割の場合は終了データを保全  それ以外のメッシュの分割
                else:
                    rmid = []
                    for tgid in ( lower_ids):
                        feedback.pushConsoleInfo( "lower id  " +str( tgid )  )

                           #  next_output   code   の下3桁　削除   C27210-02    -> C27210   が last_output の code 番号
                           #  next_output  では last_output  が同じ番号の最大4メッシュを削除する

                           # リミットより小さいレコードは旧レコードを退避
                           #  リミットにひっかかるレコードを再処理用リストから削除（同一親メッシュのものも削除）
                       #   不均等分割でリミット以下のデータがある場合は last_output -> 分割不能抽出   next_output  分割不能削除  next_output -> last_output 代入
                        parent_code = tgid[0:-3]

                           
                        rmid.append(parent_code)

                    addfeatures = []

                    #if type(last_output) is str:
                    #    last_output =  QgsVectorLayer(last_output, "mesh", "ogr")



                    alg_paramsg_n = {
                           'LAYERS': last_output,
                           'OVERWRITE': False,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
                    lmesh  = processing.run('native:package', alg_paramsg_n, context=context, feedback=feedback, is_child_algorithm=True)  


                    #last_output.removeSelection()
                    last_output = lmesh["OUTPUT"]

                    if type(last_output) is str:
                        last_output =  QgsVectorLayer(last_output, "mesh", "ogr")

                    last_output.selectAll()


                    for lf in  last_output.getFeatures():

                        for pcode in ( rmid ):
                        #    feedback.pushConsoleInfo( "pcode  " + pcode+ " meshid =" + lf[meshid]  )
                            if lf[meshid]== pcode:
                                lf["fid"] = None

                                if not lf[value_column] :
                                    lf[value_column] = 0.0

                                
                                if lf[meshid]  not in appended :

                                    addfeatures.append(lf)
                                    appended[lf[meshid]]  = lf


                                   #addfeatures.append(lf)
                                    feedback.pushConsoleInfo( "add feature   " + pcode  )


                    resLayer.dataProvider().addFeatures( addfeatures)



                    deleteFeatures = []

                    if type(next_output) is str:
                        next_output =  QgsVectorLayer(next_output, "mesh", "ogr")

                    for nf in  next_output.getFeatures():

                        for pcode in ( rmid ):
                            if nf[meshid][0:-3]== pcode:
                                deleteFeatures.append(nf.id())
                                feedback.pushConsoleInfo( "delete id  " +str( pcode )  )
                                    
                    next_output.dataProvider().deleteFeatures(deleteFeatures)


                    last_output = next_output


        
        # Return the results of the algorithm. In this case our only result is
        # the feature sink which contains the processed features, but some
        # algorithms may return multiple feature sinks, calculated numeric
        # statistics, etc. These should all be included in the returned
        # dictionary, with keys matching the feature corresponding parameter
        # or output names.

        #  不均等分割の場合   最終作業レイヤの地物がはいってないかも
        if  uneven_div:

            alg_paramsg_n = {
                           'LAYERS': next_output,
                           'OVERWRITE': False,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
            lmesh  = processing.run('native:package', alg_paramsg_n, context=context, feedback=feedback, is_child_algorithm=True)  


                    #last_output.removeSelection()
            last_output = lmesh["OUTPUT"]

            if type(last_output) is str:
                last_output =  QgsVectorLayer(last_output, "mesh", "ogr")

                last_output.selectAll()

            addfeatures = []

            for lf in  last_output.getFeatures():

                feedback.pushConsoleInfo( "add features  meshid =" + lf[meshid]  )
                lf["fid"] = None
                if not lf[value_column]:
                    lf[value_column]=0.0


                if lf[meshid]  not in appended:

                    addfeatures.append(lf)
                    appended[lf[meshid]]  = lf

                #addfeatures.append(lf)



            resLayer.dataProvider().addFeatures( addfeatures)


            


                   # フォーマット変換（gdal_translate）
            alg_params = {
            'INPUT': resLayer,
            'OPTIONS': '',
            'OUTPUT': parameters['OUTPUT']
               }
            ocv = processing.run('gdal:convertformat', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

            results["OUTPUT"] = ocv["OUTPUT"]
            return  results

       #   均等分割の場合
        else:

       
       # フォーマット変換（gdal_translate）
             alg_params = {
            'INPUT': last_output,
            'OPTIONS': '',
            'OUTPUT': parameters['OUTPUT']
             }
             ocv = processing.run('gdal:convertformat', alg_params, context=context, feedback=feedback, is_child_algorithm=True)



             results["OUTPUT"] = ocv["OUTPUT"]
             return  results


    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm. This
        string should be fixed for the algorithm, and must not be localised.
        The name should be unique within each provider. Names should contain
        lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'CSVStatMeshAggrePopProcessingAlgorithm'

    def displayName(self):
        """
        Returns the translated algorithm name, which should be used for any
        user-visible display of the algorithm name.
        """
        return 'CSVメッシュ別集計(人口按分)'

    def group(self):
        """
        Returns the name of the group this algorithm belongs to. This string
        should be localised.
        """
        return '集計'

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs to. This
        string should be fixed for the algorithm, and must not be localised.
        The group id should be unique within each provider. Group id should
        contain lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return self.tr('Aggregate')

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return CSVStatMeshAggrePopProcessingAlgorithm()