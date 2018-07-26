#include<stdlib.h>
#include<string.h>
#include "dberror.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdio.h>

typedef struct BM_PageArray {

    int pageNumber;
    int index;
    struct BM_PageArray* next;

  }BM_PageArray;

typedef struct BM_PageContent {
    //check if dirty
    int dirty;
    //initial count
    int fixCount;
    
  }BM_PageContent;

//Define the structure of buffer manager
typedef struct BM_DataManager {
    

    //record the order of writting into the buffer
    BM_PageArray *pageHeader;
    BM_PageArray *pageTail;   
    //initial total pages
    int totalPage;
    //initial read pages
    int totalRead;
    //initial write pages
    int totalWrite;
    //page array
    BM_PageHandle handleData[10000];
    BM_PageContent  content[10000]; 
    BM_PageArray PageArray[10000];


  }BM_DataManager;



RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                   const int numPages, ReplacementStrategy strategy,
                   void *stratData)
{
    //Check bm exist or not
    if (bm == NULL)
    return RC_BAD_PARAMETER;
    // check number pages
    if (numPages == 0)
    return RC_BAD_PARAMETER;
    //check file exist or not
    if( pageFileName == NULL)
    return RC_BM_NON_EXISTING_PAGE;
    //init bm pool
    bm->numPages = numPages;
    bm->pageFile = (char*)pageFileName;
    bm->strategy = strategy;
    //giving the space to size 'numPages' to store page

    BM_DataManager *data = (BM_DataManager *)malloc(sizeof(BM_DataManager));
    data->pageHeader = NULL;
    data->pageTail = NULL;
    data->totalPage = 0;
    data->totalRead = 0;
    data->totalWrite = 0;

    for(int i = 0; i < numPages; i++)
    {
        (data->content[i]).dirty = 0;
        (data->content[i]).fixCount = 0;
    }
    bm->mgmtData = data;
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    // initial buffer pool
    int *res = (int *)malloc(bm->numPages * sizeof(PageNumber));
    BM_DataManager *mydata = bm->mgmtData;
    //write dirty pages to memory.
    forceFlushPool(bm);

    for(int i = 0; i < bm->numPages; i++) {
        if(mydata->content[i].fixCount != 0) {
            return RC_WRITE_FAILED;
        }
    }

    free(mydata);
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    //initial buffer pool
    int *res = (int *)malloc(bm->numPages * sizeof(PageNumber));
    BM_DataManager *mydata = bm->mgmtData;
    //write all dirty pages to memory blocks
    for(int i = 0; i < mydata->totalPage; i++)
  {
      if(mydata->content[i].dirty == 1 && mydata->content[i].fixCount == 0){
   //      SM_FileHandle fileHandle;
     //    openPageFile(bm->pageFile, &fileHandle);
//
 //       writeBlock(mydata->handleData[i].pageNum, &fileHandle,   mydata->handleData[i].data);
        mydata->content[i].dirty = 0;
 //       mydata->totalWrite++;

      }

  }
  return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //check if page exist
    if (bm->numPages <= 0)
    {
        return RC_BM_NON_EXISTING_PAGE ;
    }
    BM_DataManager *dm = bm->mgmtData;
    int i;
    //mark modified page as dirty
    for(i = 0; i < dm->totalPage; i++)
    {
        if(dm->handleData[i].pageNum == page->pageNum)    
            dm->content[i].dirty = 1;
    }
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //check if page exist
    if (bm->numPages <= 0)
    {
        return RC_BM_NON_EXISTING_PAGE ;
    }
    BM_DataManager *dm = bm->mgmtData;
    int i;
    //notify buffer manager no need of the page once done with the reading or modifying, decrease the fix count and increase the total write count
    for(i = 0; i < dm->totalPage; i++)
    {
        if(dm->handleData[i].pageNum == page->pageNum)
        {
            if(dm->content[i].fixCount>=0&&dm->content[i].dirty == 1)
            dm->totalWrite++;
	    dm->content[i].fixCount = 0;
            
        }
    }
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    //check if page exist
    if (bm->numPages <= 0)
    {
        return RC_BM_NON_EXISTING_PAGE ;
    }
    BM_DataManager *dm = bm->mgmtData;
    int i;
    //write current page back to the page file on disk
    for(int i = 0; i < dm->totalPage; i++)
    {
        if(dm->handleData[i].pageNum == page->pageNum)
        {
	    SM_FileHandle fileHandle;
            openPageFile(bm->pageFile, &fileHandle);
            writeBlock(dm->handleData[i].pageNum, &fileHandle,dm->handleData[i].data);
            dm->totalWrite++;
	}
    }    
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
             const PageNumber pageNum)
{
    //check if page exist
    if (bm->numPages <= 0)
    {
        return RC_BM_NON_EXISTING_PAGE ;
    }
    //assign page number and page size to the PageHandle page
    BM_DataManager *dm = bm->mgmtData;
    int FIFO = 0, LRU = 0;
    int i;
    page->pageNum = pageNum;
    page->data = (char*)calloc(PAGE_SIZE,sizeof(char));
    
    //check all the occupied page frames in the buffer pool
    for(i = 0; i < dm->totalPage; i++)
    {
        //store the buffer pool's data to ph one by one

        //if the requested page already cached in a page frame, buffer simply returns the pointer
	if(dm->handleData[i].pageNum == pageNum)//check if the requested page number is within the buffer pool already
	switch(bm->strategy)
			{
			
				case RS_FIFO: 
					FIFO = 1;
					dm->content[i].fixCount++;
					break;
				
				case RS_LRU: 
					LRU = 1;
					dm->content[i].fixCount++;
					break;
			}


    }

    //if the page not cached in page frame, the buffer manager has to read this page from disk and store it to the buffer
    if(FIFO == 0)
    {
        //cache the new page into buffer if space available
        BM_PageArray *arr;
	arr = (BM_PageArray*)calloc(PAGE_SIZE, sizeof(BM_PageArray));

	//attach the array to buffer tail if the buffer is not empty
        if(dm->pageHeader != NULL)
	{
	    dm->pageTail->next = arr;
            dm->pageTail = arr;
	}
	//store the array into buffer's header if the buffer pool is empty
	else
	{
	    dm->pageHeader = arr;
            dm->pageTail = arr;
	}
        //if the page is not cached in the pool, buffer manager should read page from disk and load the requested page into page frame
        if(dm->totalPage < bm->numPages)
        {
	    arr->index = dm->totalPage;
            dm->handleData[dm->totalPage].pageNum = pageNum;
            dm->handleData[dm->totalPage].data = page->data;          
 	    dm->totalRead++;            
 	    dm->totalPage++;
        }
        else
        {
            { 
		int frontPageNum = -1;
		BM_PageArray *previous = NULL;
                BM_PageArray *current = dm->pageHeader;              
                while(current != NULL)
                {
                    int index = current->index;
		    if(bm->strategy == RS_FIFO)
		    {
			   if(dm->content[index].fixCount == 0)
         	           {
               		         if(previous != NULL)  
		        	    previous->next = current->next;
				 else
                            	    dm->pageHeader = dm->pageHeader->next;
				 if(LRU == 1)
			            arr->index = index;
				 else
			    	    frontPageNum = index;
                        	 break;
			    }
                    	}
		    else if(LRU == 0)
		    {
                        if(previous != NULL)  
		            previous->next = current->next;
			else
                            dm->pageHeader = dm->pageHeader->next;
			    frontPageNum = index;
                        break;
                    }
		    else if(dm->handleData[index].pageNum == pageNum)
		    {
                        if(previous != NULL)  
		            previous->next = current->next;
			else
                            dm->pageHeader = dm->pageHeader->next;
			if(LRU == 1)
			    arr->index = index;
			else
			    frontPageNum = index;
                        break;

                    }	                 
                    previous = current;
                    current = current->next;
                }

                //assign the new page to the first position of the array
                if(frontPageNum >= 0)
                {
                    arr->index = frontPageNum;
		    dm->handleData[frontPageNum].pageNum = pageNum;
		    dm->handleData[frontPageNum].data = page->data;
                    dm->totalRead++;
                }
            }
        }

	sprintf(page->data, "%s-%i", "Page", page->pageNum);//store the page number into the page frame's pointer
        
    }
    
    return RC_OK;
}


