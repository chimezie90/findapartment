{ pkgs }: {
  deps = [
    pkgs.sqlite
    pkgs.python312
    pkgs.python312Packages.pip
    pkgs.python312Packages.flask
    pkgs.python312Packages.requests
    pkgs.python312Packages.beautifulsoup4
    pkgs.python312Packages.pyyaml
    pkgs.python312Packages.python-dotenv
  ];
}
