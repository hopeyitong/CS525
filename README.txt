/////////////////////////////
/* Buffer Manager Interface Access Pages */
/////////////////////////////

1. markDirty 
1) check if the page exist
2) mark modified page ad dirty, set dirty bit to 1

2. unpinPag
1) check if page exist
2) if the client finishing using the page, notify buffer manager no need of the page once done with the reading or modifying, decrease the fix count and increase the total write count

3. forcePage
1) check if page exist
2) write current page back to the page file on disk, using functions openPageFile and writeBlock in assignment 1

4. pinPage
1) check if page exist
2) assign page number and page size to the PageHandle page
3) check all the occupied page frames in the buffer pool, if the requested page already cached in a page frame, buffer simply returns the pointer
4) if the page not cached in page frame, the buffer manager has to read this page from disk and store it to the buffer
5) store the page number into the page frame's pointer
