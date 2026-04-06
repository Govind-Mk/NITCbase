#include "StaticBuffer.h"
#include<string.h>
// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer() {
  unsigned char buffer[BLOCK_SIZE];
  for(int i=0;i<4;i++)
  {
    Disk::readBlock(buffer,i);
    memcpy(blockAllocMap+i*BLOCK_SIZE,buffer,BLOCK_SIZE);
  }
  // initialise all blocks as free
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}


StaticBuffer::~StaticBuffer() {
  unsigned char buffer[BLOCK_SIZE];
  for(int i=0;i<4;i++)
  {
    memcpy(buffer,blockAllocMap+i*BLOCK_SIZE,BLOCK_SIZE);
    Disk::writeBlock(buffer,i);
  }
  /*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
   unsigned char *bufferptr;
   for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
   {
    if(metainfo[bufferIndex].free==false && metainfo[bufferIndex].dirty==true)
    {
      bufferptr=blocks[bufferIndex];
      Disk::writeBlock(bufferptr,metainfo[bufferIndex].blockNum);
    }
   }
}


int StaticBuffer::getFreeBuffer(int blockNum){
    // Check if blockNum is valid (non zero and less than DISK_BLOCKS)
    // and return E_OUTOFBOUND if not valid.
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
      return E_OUTOFBOUND;
    }
    // increase the timeStamp in metaInfo of all occupied buffers.
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
      if(metainfo[i].free==false)
      {
        metainfo[i].timeStamp+=1;
      }
    }
    // let bufferNum be used to store the buffer number of the free/freed buffer.
    int bufferNum;
    int allocatedBuffer,max=0;
    // iterate through metainfo and check if there is any buffer free
    for(allocatedBuffer=0;allocatedBuffer<BUFFER_CAPACITY;allocatedBuffer++)
    {
      if(metainfo[max].timeStamp<metainfo[allocatedBuffer].timeStamp)
      {
        max=allocatedBuffer;
      }
      if(metainfo[allocatedBuffer].free==true)
      {
        break;
      }
    }
    // if a free buffer is available, set bufferNum = index of that free buffer.
    if(allocatedBuffer!=BUFFER_CAPACITY)
    {
      bufferNum=allocatedBuffer;
    }
    else 
    {
      if(metainfo[max].dirty==true)
        Disk::writeBlock(blocks[max],metainfo[max].blockNum);
      bufferNum=max;
    }
      metainfo[bufferNum].blockNum=blockNum;
      metainfo[bufferNum].dirty=false;
      metainfo[bufferNum].free=false;
      metainfo[bufferNum].timeStamp=0;
      
    // if a free buffer is not available,
    //     find the buffer with the largest timestamp
    //     IF IT IS DIRTY, write back to the disk using Disk::writeBlock()
    //     set bufferNum = index of this buffer

    // update the metaInfo entry corresponding to bufferNum with
    // free:false, dirty:false, blockNum:the input block number, timeStamp:0.

    // return the bufferNum.
    return bufferNum;
}
/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
    if(blockNum<0 || blockNum>=DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }
  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].free==false && metainfo[i].blockNum==blockNum)
        {
            return i;
        }
    }
  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
  int bufferNum = getBufferNum(blockNum);
    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
  if(bufferNum==E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;
    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
  if(bufferNum==E_OUTOFBOUND)
    return E_OUTOFBOUND;
  else{
    metainfo[bufferNum].dirty=true;
  }
    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo

    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.
if(blockNum<0 || blockNum>=DISK_BLOCKS)
  {
    return E_OUTOFBOUND;
  }
  return (int)blockAllocMap[blockNum];
    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
}