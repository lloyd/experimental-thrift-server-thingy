namespace go   reverse

service Reverse {
  string Do(1: string input)

  void DoNothing()

  string DoReturn()

  void DoArg(1: string input)
}
