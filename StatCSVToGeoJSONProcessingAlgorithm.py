"""
/***************************************************************************
 StatCSVProcessing
                                 A QGIS plugin
 Read a CSV file and calculate the number of lines for each item in the specified column
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                              -------------------
        begin                : 2020-07-10
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
__date__ = '2020-07-10'
__copyright__ = '(C) 2020 by Yoichi Kayama'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'


from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterEnum
from qgis.core import QgsProcessingParameterVectorLayer
from qgis.core import QgsProcessingParameterFile
from qgis.core import QgsProcessingParameterField
from qgis.core import QgsProcessingParameterVectorDestination
import processing


class StatCsvProcessingToGeoJsonAlgorithm(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterVectorLayer('addresslayer', '住所レイヤ', types=[QgsProcessing.TypeVectorPolygon], defaultValue=None))
        self.addParameter(QgsProcessingParameterField('addressfield', '住所フィールド', type=QgsProcessingParameterField.String, parentLayerParameterName='addresslayer', allowMultiple=False, defaultValue=None))

        #self.addParameter(QgsProcessingParameterFile('CSVfile', 'CSV file', behavior=QgsProcessingParameterFile.File, fileFilter='All Files（*.*）', defaultValue=None))
        self.addParameter(
            QgsProcessingParameterFile(
                'CSVfile',
                '入力csvファイル指定',
                extension='csv'
            )
         )


        self.addParameter(QgsProcessingParameterEnum('encode', 'ENCODE', options=['SJIS','UTF-8'], allowMultiple=False, defaultValue=None))

        self.addParameter(QgsProcessingParameterVectorDestination('GeojsonOutput', '出力geojsonファイル名', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(5, model_feedback)
        results = {}
        outputs = {}

        # CSVtoStatProcessing
        alg_params = {
            'ENCODING': parameters['encode'],
            'INPUT': parameters['CSVfile'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Csvtostatprocessing'] = processing.run('QGIS_stat:CSVtoStatProcessing', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # 属性テーブルで結合（table join）
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': parameters['addressfield'],
            'FIELDS_TO_COPY': None,
            'FIELD_2': 'address',
            'INPUT': parameters['addresslayer'],
            'INPUT_2': outputs['Csvtostatprocessing']['OUTPUT'],
            'METHOD': 1,
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['TableJoin'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # レイヤをGeoPackage化
        alg_params = {
            'LAYERS': outputs['TableJoin']['OUTPUT'],
            'OVERWRITE': True,
            'SAVE_STYLES': False,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Geopackage'] = processing.run('native:package', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # SpatiaLiteでSQLを実行
        alg_params = {
            'DATABASE': outputs['Geopackage']['OUTPUT'],
            'SQL': 'update \"出力レイヤ\" set count=0 where count is NULL'
        }
        outputs['Spatialitesql'] = processing.run('qgis:spatialiteexecutesql', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # フォーマット変換（gdal_translate）
        alg_params = {
            'INPUT': outputs['Geopackage']['OUTPUT'],
            'OPTIONS': '',
            'OUTPUT': parameters['GeojsonOutput']
        }
        outputs['Gdal_translate'] = processing.run('gdal:convertformat', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['GeojsonOutput'] = outputs['Gdal_translate']['OUTPUT']
        return results

    def name(self):
        return 'Stat_CSV'

    def displayName(self):
        return 'CSV住所別集計（GeoJSON出力)'

    def group(self):
        return 'Aggregate'

    def groupId(self):
        return 'Aggregate'

    def createInstance(self):
        return StatCsvProcessingToGeoJsonAlgorithm()
