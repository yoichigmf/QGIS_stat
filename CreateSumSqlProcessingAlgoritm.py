# -*- coding: utf-8 -*-

"""
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

_author__ = 'Yoichi Kayama'
__date__ = '2020-07-15'
__copyright__ = '(C) 2020 by Yoichi Kayama'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'


from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterField,
                       QgsProcessingParameterString,
                       QgsProcessingParameterFeatureSink)
from qgis import processing


class CreateSumSqlProcessingAlgorithm(QgsProcessingAlgorithm):
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

    INPUT = 'INPUT'

    FIELD1 = 'FIELD1'
    FIELD2 = 'FIELD2'

    OUTPUT = 'OUTPUT'

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return CreateSumSqlProcessingAlgorithm()

    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm. This
        string should be fixed for the algorithm, and must not be localised.
        The name should be unique within each provider. Names should contain
        lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'CreateSumSQL'

    def displayName(self):
        """
        Returns the translated algorithm name, which should be used for any
        user-visible display of the algorithm name.
        """
        return self.tr('CreateSumSQL')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to. This string
        should be localised.
        """
        return self.tr('Aggregate')

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs to. This
        string should be fixed for the algorithm, and must not be localised.
        The group id should be unique within each provider. Group id should
        contain lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'Aggregate'

    def shortHelpString(self):
        """
        Returns a localised short helper string for the algorithm. This string
        should provide a basic description about what the algorithm does and the
        parameters and outputs associated with it..
        """
        return self.tr("Example algorithm short description")

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """

        # We add the input vector features source. It can have any kind of
        # geometry.
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT,
                self.tr('Input layer'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )


        self.addParameter(
            QgsProcessingParameterField(
                self.FIELD1,
                self.tr('Address Field'),
                '',
                self.INPUT
            )
        )


        self.addParameter(
            QgsProcessingParameterField(
                self.FIELD2,
                self.tr('population Field'),
                '',
                self.INPUT
            )
        )




        # We add a feature sink in which to store our processed features (this
        # usually takes the form of a newly created vector layer when the
        # algorithm is run in QGIS).
        #self.addParameter(
        #    QgsProcessingParameterString(
        #        self.OUTPUT,
        #        self.tr('Output SQL')
        #    )
        #)

    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """

        # Retrieve the feature source and sink. The 'dest_id' variable is used
        # to uniquely identify the feature sink, and must be included in the
        # dictionary returned by the processAlgorithm function.
        source = self.parameterAsSource(
            parameters,
            self.INPUT,
            context
        )

        # If source was not found, throw an exception to indicate that the algorithm
        # encountered a fatal error. The exception text can be any string, but in this
        # case we use the pre-built invalidSourceError method to return a standard
        # helper text for when a source cannot be evaluated
        if source is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT))

        #out_string = self.parameterAsString(parameters, self.OUTPUT, context)

        #table_name = self.parameterAsString(
        #    parameters,
        #    self.INPUT,
        #    context
        #)

        table_name = source.sourceName()

        address_f  = self.parameterAsString(
            parameters,
            self.FIELD1,
            context
        )


        pop_f  = self.parameterAsString(
            parameters,
            self.FIELD2,
            context
        )


        sqlstring = 'create table tmp_sum as select "' + address_f + '" address, sum("' + pop_f + '") pop_t from "' + table_name + '" group by "' + address_f  +'"'

        # Return the results of the algorithm. In this case our only result is
        # the feature sink which contains the processed features, but some
        # algorithms may return multiple feature sinks, calculated numeric
        # statistics, etc. These should all be included in the returned
        # dictionary, with keys matching the feature corresponding parameter


        # メッシュ単位人口の集計
        alg_params = {
                    'DATABASE': source,
                    'SQL': sqlstring
                }

        processing.run('qgis:spatialiteexecutesql', alg_params, context=context, feedback=feedback)


        nsql =  'create table tmp_sum as select "' + address_f + '" address, sum("' + pop_f + '") pop_t from "' + table_name + '" group by "' + address_f  +'"'

        alg2_params = {
                    'DATABASE': source,
                    'SQL': nsql
                }

        processing.run('qgis:spatialiteexecutesql', alg2_params, context=context, feedback=feedback)


        #   元テーブルに行政区人口カラム、比率カラムを追加



        #   集計単位と元テーブルの結合


        # or output names.
        return {self.OUTPUT: out_string}
