service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void putBackup(1: string key, 2: string value);
  map<string, string> getMap();
  void fullCopyFromPrimaryToBackup();
  void setPrimary(1 : bool primary);
}
