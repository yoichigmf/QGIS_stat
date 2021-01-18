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

class CalcPopfromMeshAlgorithm(QgsProcessingAlgorithm):
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

    #proportional_div = ['人口','面積']


    def initAlgorithm(self, config):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """
    
        self.addParameter(QgsProcessingParameterVectorLayer('addresslayer', '住所レイヤ',
                         types=[QgsProcessing.TypeVectorPolygon], defaultValue=None))
        self.addParameter(QgsProcessingParameterField('addressfield', '住所フィールド', 
                         type=QgsProcessingParameterField.String, parentLayerParameterName='addresslayer', allowMultiple=False, defaultValue=None))

    

        self.addParameter(QgsProcessingParameterVectorLayer('popmeshlayer', '人口メッシュレイヤ',
                         types=[QgsProcessing.TypeVectorPolygon], optional=False, defaultValue=None))
        
        self.addParameter(QgsProcessingParameterField('popmeshid', '人口メッシュIDフィールド', 
                         type=QgsProcessingParameterField.String, parentLayerParameterName='popmeshlayer', optional=False, allowMultiple=False))


        self.addParameter(QgsProcessingParameterField('popmeshpop', '人口メッシュ人口フィールド', 
                         type=QgsProcessingParameterField.Numeric, parentLayerParameterName='popmeshlayer', optional=False, allowMultiple=False))


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

        feedback = QgsProcessingMultiStepFeedback(5, model_feedback)



        addressLayer = self.parameterAsVectorLayer(
            parameters,
            "addresslayer",
            context
        )
        addressfields = self.parameterAsFields  (
             parameters,
             'addressfield',
             context
        )


        popmeshLayer = self.parameterAsVectorLayer(
            parameters,
            "popmeshlayer",
            context
        )
        if popmeshLayer  is None:
            raise QgsProcessingException(self.tr('popmesh layer missed'))

        popmeshidfields = self.parameterAsFields  (
             parameters,
             'popmeshid',
             context
        )

        popmeshpopfields = self.parameterAsFields  (
             parameters,
             'popmeshpop',
             context
        )                  

  

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        

  
        meshid = popmeshidfields[0]

           #   行政界の面積計算
    #    
    #  面積出力フィールド名

        area_column = 'mesh_area'

        params3 = { 'INPUT' :  popmeshLayer, 'FIELD_NAME' : area_column , 'FIELD_TYPE': 0, 'FIELD_LENGTH':12, 'FIELD_PRECISION':5, 
                 'NEW_FIELD':1,'FORMULA':'$area','OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }

        res3 = processing.run('qgis:fieldcalculator', params3, context=context, feedback=feedback ,is_child_algorithm=True)

        if feedback.isCanceled():
            return {}

        feedback.pushConsoleInfo( "caluculate area OK "  )

    #   ここから関数化がいいかも
    #   メッシュと行政界のIntesect
        feedback.setCurrentStep(2)
     
        params2 = { 'INPUT' :res3["OUTPUT"], 'INPUT_FIELDS' : [], 
                'OUTPUT' : QgsProcessing.TEMPORARY_OUTPUT, 'OVERLAY' : addressLayer, 'OVERLAY_FIELDS' : [] }
                #              'OUTPUT' : parameters["OUTPUT"], 'OVERLAY' : res3["OUTPUT"], 'OVERLAY_FIELDS' : [] }

        res2 = processing.run('qgis:union', params2,  context=context, feedback=feedback ,is_child_algorithm=True)
        if feedback.isCanceled():
            return {}

        feedback.pushConsoleInfo( "union  OK "  )
    #   union ポリゴンの面積計算

        feedback.setCurrentStep(3)
        params_del = { 'INPUT' :  res2["OUTPUT"], 'COLUMN' : ['fid'] , 
                'OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }
                #'OUTPUT' : parameters["OUTPUT"] }

        res_del = processing.run('qgis:deletecolumn', params_del, context=context, feedback=feedback ,is_child_algorithm=True)
        if feedback.isCanceled():
            return {}

        feedback.pushConsoleInfo( "delete column  OK "  )    
        feedback.setCurrentStep(4)

        alg_paramsg_n = {
                           'LAYERS': res_del["OUTPUT"],
                           'OVERWRITE': False,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
        res_n  = processing.run('native:package', alg_paramsg_n, context=context, feedback=feedback, is_child_algorithm=True)  


        if feedback.isCanceled():
            return {}

        feedback.pushConsoleInfo( "convert to geopackage   OK "  )   
        feedback.setCurrentStep(5)

        area_column2 = 'div_area'
 
        params4 = { 'INPUT' :  res_n["OUTPUT"], 'FIELD_NAME' : area_column2 , 'FIELD_TYPE': 0, 'FIELD_LENGTH':12, 'FIELD_PRECISION':5, 
                 'NEW_FIELD':1,'FORMULA':'$area','OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }
        #         'NEW_FIELD':1,'FORMULA':'$area','OUTPUT' : parameters["OUTPUT"] }

        res4 = processing.run('qgis:fieldcalculator', params4, context=context, feedback=feedback ,is_child_algorithm=True)


        #   分割ポリゴンの面積と元ポリゴンの面積の比率にメッシュ人口をかけて分割ポリゴンの想定人口を算出する
        ppopfield  = popmeshpopfields[0]
        new_column = 'snum'

        exp_str = area_column2 + "/" + area_column + "*" + ppopfield


        params5 = { 'INPUT' :  res4["OUTPUT"], 'FIELD_NAME' : new_column , 'FIELD_TYPE': 0, 'FIELD_LENGTH':12, 'FIELD_PRECISION':5, 
         #        'NEW_FIELD':1,'FORMULA':exp_str ,'OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }
                 'NEW_FIELD':1,'FORMULA':exp_str,'OUTPUT' : parameters["OUTPUT"] }

        res5 = processing.run('qgis:fieldcalculator', params5, context=context, feedback=feedback ,is_child_algorithm=True)


        results["OUTPUT"] = res5["OUTPUT"]
        return  results


        feedback.pushConsoleInfo( "snum calc end "   )

        params_del2 = { 'INPUT' :  res5["OUTPUT"], 'COLUMN' : ['fid'] , 
                'OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }
                #'OUTPUT' : parameters["OUTPUT"] }

        res_del2 = processing.run('qgis:deletecolumn', params_del2, context=context, feedback=feedback ,is_child_algorithm=True)
        if feedback.isCanceled():
            return {}

        feedback.pushConsoleInfo( "delete column 2 OK "  )    

        feedback.setCurrentStep(6)


        alg_paramsg_n2 = {
                           'LAYERS': res_del2["OUTPUT"],
                           'OVERWRITE': False,
                          'SAVE_STYLES': False,
                           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
                           }
        res_n2  = processing.run('native:package', alg_paramsg_n2, context=context, feedback=feedback, is_child_algorithm=True)  




        
        tgLayer = res5["OUTPUT"]

   

        if type(tgLayer) is str:
            feedback.pushConsoleInfo( "tglayer is string"   )
            tgLayer =  QgsVectorLayer(tgLayer, "union", "memory")


        agar = []
        #  フィールド単位に集計方法を指定している
        for field in tgLayer.fields():

            agreg = {}
            feedback.pushConsoleInfo( "name "  + field.name() )
            if field.name() != "fid":

                agreg['input'] = '"' + field.name() + '"'


              
                agreg['name']  = field.name()
                agreg['aggregate'] = 'first_value'

                agreg['length'] =field.length()
                agreg['precision'] =field.precision()     
                agreg['type'] =field.type()   

                if field.name() == new_column:
                     agreg['aggregate'] = 'sum'

                agar.append(agreg)


        addressf = addressfields[0] 

        #    集計
        #params6 = { 'INPUT' : res5["OUTPUT"], 'GROUP_BY' : addressf, 'AGGREGATES': agar, 'OUTPUT' :QgsProcessing.TEMPORARY_OUTPUT }
        params6 = { 'INPUT' : res5["OUTPUT"], 'GROUP_BY' : addressf, 'AGGREGATES': agar, 'OUTPUT' :parameters["OUTPUT"] }
        feedback.pushConsoleInfo( "aggregate "  )
        res8 = processing.run('qgis:aggregate', params6,  context=context, feedback=feedback ,is_child_algorithm=True)

        if feedback.isCanceled():
            return {}
        feedback.pushConsoleInfo( "aggregate OK "  )

    


    #   レイヤ結合　　qgis:joinattributestable
        #QgsProject.instance().addMapLayer(res7["OUTPUT"])

        param7 = { 'DISCARD_NONMATCHING' : False, 'FIELD' : addressf, 'FIELDS_TO_COPY' : [new_column], 'FIELD_2' : addressf, 
             'INPUT' : addressLayer, 
             'INPUT_2' : res8['OUTPUT'], 'METHOD' : 1, 'OUTPUT' : parameters["OUTPUT"], 'PREFIX' : '' }



        res9 = processing.run('qgis:joinattributestable', param7,  context=context, feedback=feedback ,is_child_algorithm=True)

        if feedback.isCanceled():
            return {}
        feedback.pushConsoleInfo( "joinattributetable OK"  )




        # Return the results of the algorithm. In this case our only result is
        # the feature sink which contains the processed features, but some
        # algorithms may return multiple feature sinks, calculated numeric
        # statistics, etc. These should all be included in the returned
        # dictionary, with keys matching the feature corresponding parameter
        # or output names.

     
       # フォーマット変換（gdal_translate）
        alg_params = {
            'INPUT': res2["OUTPUT"],
            'OPTIONS': '',
            'OUTPUT': parameters['OUTPUT']
             }
       # ocv = processing.run('gdal:convertformat', alg_params, context=context, feedback=feedback, is_child_algorithm=True)



        results["OUTPUT"] = res9["OUTPUT"]
        return  results


    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm. This
        string should be fixed for the algorithm, and must not be localised.
        The name should be unique within each provider. Names should contain
        lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'CalcPopfromMeshAlgorithm'

    def displayName(self):
        """
        Returns the translated algorithm name, which should be used for any
        user-visible display of the algorithm name.
        """
        return '行政界人口集計(メッシュ入力)'

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
        return CalcPopfromMeshAlgorithm()