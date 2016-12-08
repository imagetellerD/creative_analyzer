# -*- coding: utf-8 -*-
#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class ImageDataType(object):
  """
  图片数据类型
  """
  IDT_URL = 1
  IDT_PATH = 2
  IDT_BINARY = 3

  _VALUES_TO_NAMES = {
    1: "IDT_URL",
    2: "IDT_PATH",
    3: "IDT_BINARY",
  }

  _NAMES_TO_VALUES = {
    "IDT_URL": 1,
    "IDT_PATH": 2,
    "IDT_BINARY": 3,
  }


class Test(object):
  """
  Attributes:
   - id: test id
   - name: test name
  """

  thrift_spec = (
    None, # 0
    (1, TType.I64, 'id', None, None, ), # 1
    (2, TType.STRING, 'name', None, None, ), # 2
  )

  def __init__(self, id=None, name=None,):
    self.id = id
    self.name = name

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I64:
          self.id = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.name = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('Test')
    if self.id != None:
      oprot.writeFieldBegin('id', TType.I64, 1)
      oprot.writeI64(self.id)
      oprot.writeFieldEnd()
    if self.name != None:
      oprot.writeFieldBegin('name', TType.STRING, 2)
      oprot.writeString(self.name)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ImageTag(object):
  """
  Attributes:
   - tag: 图片tag
   - confidence: 置信度/自信度 0-100
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'tag', None, None, ), # 1
    (2, TType.I16, 'confidence', None, None, ), # 2
  )

  def __init__(self, tag=None, confidence=None,):
    self.tag = tag
    self.confidence = confidence

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.tag = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I16:
          self.confidence = iprot.readI16();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ImageTag')
    if self.tag != None:
      oprot.writeFieldBegin('tag', TType.STRING, 1)
      oprot.writeString(self.tag)
      oprot.writeFieldEnd()
    if self.confidence != None:
      oprot.writeFieldBegin('confidence', TType.I16, 2)
      oprot.writeI16(self.confidence)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ImageAnalyzeResult(object):
  """
  Attributes:
   - tags: 图片tags
   - descriptions: 图片描述
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'tags', (TType.STRUCT,(ImageTag, ImageTag.thrift_spec)), None, ), # 1
    (2, TType.LIST, 'descriptions', (TType.STRING,None), None, ), # 2
  )

  def __init__(self, tags=None, descriptions=None,):
    self.tags = tags
    self.descriptions = descriptions

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.tags = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = ImageTag()
            _elem5.read(iprot)
            self.tags.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.LIST:
          self.descriptions = []
          (_etype9, _size6) = iprot.readListBegin()
          for _i10 in xrange(_size6):
            _elem11 = iprot.readString();
            self.descriptions.append(_elem11)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ImageAnalyzeResult')
    if self.tags != None:
      oprot.writeFieldBegin('tags', TType.LIST, 1)
      oprot.writeListBegin(TType.STRUCT, len(self.tags))
      for iter12 in self.tags:
        iter12.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.descriptions != None:
      oprot.writeFieldBegin('descriptions', TType.LIST, 2)
      oprot.writeListBegin(TType.STRING, len(self.descriptions))
      for iter13 in self.descriptions:
        oprot.writeString(iter13)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ImageData(object):
  """
  Attributes:
   - image_url: 图片url
   - filename: 图片文件地址，须确保是一个server可访问的目录（如同一台机器上）
   - image_binary_data: 图片2进制data
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'image_url', None, None, ), # 1
    (2, TType.STRING, 'filename', None, None, ), # 2
    (3, TType.STRING, 'image_binary_data', None, None, ), # 3
  )

  def __init__(self, image_url=None, filename=None, image_binary_data=None,):
    self.image_url = image_url
    self.filename = filename
    self.image_binary_data = image_binary_data

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.image_url = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.filename = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.image_binary_data = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ImageData')
    if self.image_url != None:
      oprot.writeFieldBegin('image_url', TType.STRING, 1)
      oprot.writeString(self.image_url)
      oprot.writeFieldEnd()
    if self.filename != None:
      oprot.writeFieldBegin('filename', TType.STRING, 2)
      oprot.writeString(self.filename)
      oprot.writeFieldEnd()
    if self.image_binary_data != None:
      oprot.writeFieldBegin('image_binary_data', TType.STRING, 3)
      oprot.writeString(self.image_binary_data)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
