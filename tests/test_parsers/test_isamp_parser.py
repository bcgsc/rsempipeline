# -*- coding: utf-8 -* 

import os
import unittest
import logging
import logging.config
import StringIO

import mock

from testfixtures import log_capture

from rsempipeline.parsers import isamp_parser
from rsempipeline.utils.objs import Series, Sample

logger = logging.getLogger('rsempipeline.parsers.soft_parser')
logger.addHandler(logging.StreamHandler())


class IsampParserTestCase(unittest.TestCase):
    def test_read_csv_gse_as_key(self):
        m  = mock.mock_open(read_data=os.linesep.join(
            [
                'GSE1,Homo sapiens,GSM1',
                'GSE1,Homo sapiens,GSM2',
                'GSE2,Homo sapiens,GSM3'
            ]))
        # use StringIO.StringIO because inf will be read by csv.reader, it must
        # return an iternator, refer to test_soft_parser.test_parse for the
        # other scenario
        m.return_value.__iter__ = lambda self: StringIO.StringIO(self.read())
        m.return_value.__next__ = lambda self: self.readline()
        with mock.patch('rsempipeline.parsers.isamp_parser.open', m):
            res = isamp_parser.read_csv_gse_as_key('infile.csv')
            self.assertEqual(res, {
                'GSE1': ['GSM1', 'GSM2'],
                'GSE2': ['GSM3']
            })

    @log_capture()
    def test_read_csv_gse_as_key_invalid_format1(self, L):
        m  = mock.mock_open(read_data=os.linesep.join(
            [
                'GSE1,GSM1',
                'GSE1,Homo sapiens,GSM2',
                'GSE2,Homo sapiens,GSM3'
            ]))
        m.return_value.__iter__ = lambda self: StringIO.StringIO(self.read())
        m.return_value.__next__ = lambda self: self.readline()
        with mock.patch('rsempipeline.parsers.isamp_parser.open', m):
            res = isamp_parser.read_csv_gse_as_key('infile.csv')
            self.assertEqual(res, {
                'GSE1': ['GSM2'],
                'GSE2': ['GSM3']
            })
            L.check(('rsempipeline.parsers.isamp_parser', 'ERROR',
                     "Ignored invalid row (1): ['GSE1', 'GSM1']"))

    @log_capture()
    def test_read_csv_gse_as_key_invalid_format2(self, L):
        m  = mock.mock_open(read_data=os.linesep.join(
            [
                'GSE,Homo sapiens,GSM1',
            ]))
        m.return_value.__iter__ = lambda self: StringIO.StringIO(self.read())
        m.return_value.__next__ = lambda self: self.readline()
        with mock.patch('rsempipeline.parsers.isamp_parser.open', m):
            res = isamp_parser.read_csv_gse_as_key('infile.csv')
            self.assertEqual(res, {})
            L.check(('rsempipeline.parsers.isamp_parser', 'ERROR',
                     "Ignored invalid row (1): ['GSE', 'Homo sapiens', 'GSM1']"))


    @log_capture()
    def test_read_csv_gse_as_key_invalid_format3(self, L):
        m  = mock.mock_open(read_data=os.linesep.join(
            [
                'GSE1,Homo sapiens,GSM',
            ]))
        m.return_value.__iter__ = lambda self: StringIO.StringIO(self.read())
        m.return_value.__next__ = lambda self: self.readline()
        with mock.patch('rsempipeline.parsers.isamp_parser.open', m):
            res = isamp_parser.read_csv_gse_as_key('infile.csv')
            self.assertEqual(res, {})
            L.check(('rsempipeline.parsers.isamp_parser', 'ERROR',
                     "Ignored invalid row (1): ['GSE1', 'Homo sapiens', 'GSM']"))

    def test_gen_isamp_from_csv(self):
        m  = mock.mock_open(read_data=os.linesep.join(
            [
                'GSE1,Homo sapiens,GSM1',
                'GSE1,Homo sapiens,GSM2',
                'GSE2,Homo sapiens,GSM3'
            ]))
        # use StringIO.StringIO because inf will be read by csv.reader, it must
        # return an iternator, refer to test_soft_parser.test_parse for the
        # other scenario
        m.return_value.__iter__ = lambda self: StringIO.StringIO(self.read())
        m.return_value.__next__ = lambda self: self.readline()
        with mock.patch('rsempipeline.parsers.isamp_parser.open', m):
            res = isamp_parser.gen_isamp_from_csv('infile.csv')
            self.assertEqual(res, {
                'GSE1': ['GSM1', 'GSM2'],
                'GSE2': ['GSM3']
            })

    def test_gen_isamp_from_str(self):
        self.assertEqual(isamp_parser.gen_isamp_from_str(
            'GSE1 GSM1; GSE1 GSM2'), {'GSE1': ['GSM1', 'GSM2']})
        self.assertEqual(isamp_parser.gen_isamp_from_str(
            'GSE1 GSM1; GSE1 GSM2 '), {'GSE1': ['GSM1', 'GSM2']})
        self.assertEqual(isamp_parser.gen_isamp_from_str(
            'GSE1 GSM1; GSE1 GSM2;'), {'GSE1': ['GSM1', 'GSM2']})
        self.assertEqual(isamp_parser.gen_isamp_from_str(
            'GSE1 GSM1'), {'GSE1': ['GSM1']})
        self.assertEqual(isamp_parser.gen_isamp_from_str(
            'GSE1 GSM1; GSE2 GSM2'), {'GSE1': ['GSM1'], 'GSE2': ['GSM2']})

    @mock.patch('rsempipeline.parsers.isamp_parser.gen_isamp_from_str')
    @mock.patch('rsempipeline.parsers.isamp_parser.gen_isamp_from_csv')
    @mock.patch.object(isamp_parser.os.path, 'exists')
    def test_get_isamp_from_invalid_csv(self, mock_exists, mock_gen_isamp_from_csv,
                                        mock_gen_isamp_from_str):
        mock_exists.return_value = True
        self.assertRaisesRegexp(
            ValueError, 'unrecognized file type of GSE_species_GSM.txt. The filename must end with .csv',
            isamp_parser.get_isamp, 'GSE_species_GSM.txt')
        self.assertFalse(mock_gen_isamp_from_csv.called)
        self.assertFalse(mock_gen_isamp_from_str.called)

    @mock.patch('rsempipeline.parsers.isamp_parser.gen_isamp_from_str')
    @mock.patch('rsempipeline.parsers.isamp_parser.gen_isamp_from_csv')
    @mock.patch.object(isamp_parser.os.path, 'exists')
    def test_get_isamp_from_csv(self, mock_exists, mock_gen_isamp_from_csv,
                                mock_gen_isamp_from_str):
        mock_exists.return_value = True
        isamp_parser.get_isamp('GSE_species_GSM.csv')
        mock_gen_isamp_from_csv.called_once_with('GSE_species_GSM.csv')
        self.assertFalse(mock_gen_isamp_from_str.called)

    @mock.patch('rsempipeline.parsers.isamp_parser.gen_isamp_from_csv')
    @mock.patch.object(isamp_parser.os.path, 'exists')
    def test_get_isamp_from_str(self, mock_exists, mock_gen_isamp_from_csv):
        mock_exists.return_value = False
        self.assertEqual(isamp_parser.get_isamp('GSE1 GSM1'), {'GSE1': ['GSM1']})
        self.assertFalse(mock_gen_isamp_from_csv.called)
