#include <cstdio>
#include "Disk_Class/Disk.h"
#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Cache/RelCacheTable.h"
#include "Cache/AttrCacheTable.h"
#include  <string.h>
#include "FrontendInterface/FrontendInterface.h"

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  return FrontendInterface::handleFrontend(argc, argv);
}
