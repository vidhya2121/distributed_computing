service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void putBackup(1: string key, 2: string value);
  void putMap(1: list<string> keys, 2: list<string> values);
  void fullCopyFromPrimaryToBackup(1: string host, 2: i32 port);
  void setPrimary(1 : bool primary);
}
