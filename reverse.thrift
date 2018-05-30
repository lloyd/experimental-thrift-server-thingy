namespace go   reverse

exception TooBusyException {
}

service Reverse {
  string Do(1: string input)
    throws (1: TooBusyException extb),

  void DoNothing(),

  string DoReturn(),

  void DoArg(1: string input),
}
