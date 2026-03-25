import 'dart:io';
import 'package:ffastdb/ffastdb.dart';
import 'package:ffastdb/src/storage/io/io_storage_strategy.dart';
import 'package:ffastdb/src/storage/page_manager.dart';

void main() async {
  final dir = await Directory.systemTemp.createTemp('fastdb_debug_');
  final path = '${dir.path}/db.fdb';

  // Session 1: write
  final db = FastDB(IoStorageStrategy(path));
  await db.open();
  final id = await db.insert({'msg': 'hello'});
  print('Inserted id=$id');
  await db.close();

  // Inspect raw bytes
  final bytes = await File(path).readAsBytes();
  print('File size: ${bytes.length}');
  final rootPage = bytes[4] | (bytes[5] << 8) | (bytes[6] << 16) | (bytes[7] << 24);
  final nextId   = bytes[8] | (bytes[9] << 8) | (bytes[10] << 16) | (bytes[11] << 24);
  print('Header: rootPage=$rootPage  nextId=$nextId  cleanFlag=0x${bytes[24].toRadixString(16)}');

  // Inspect B-Tree page 1 first 32 bytes
  final p1start = PageManager.pageSize;
  print('BTree page1 bytes[0..31]: ${bytes.sublist(p1start, p1start + 32)}');

  // Session 2: read
  final db2 = FastDB(IoStorageStrategy(path));
  await db2.open();
  final allIds = await db2.rangeSearch(1, 0x7FFFFFFF);
  print('rangeSearch(1, MAX) = $allIds');
  final doc = await db2.findById(id);
  print('findById($id) = $doc');
  await db2.close();

  await dir.delete(recursive: true);
}
