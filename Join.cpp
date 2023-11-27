#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	vector<Bucket> partitions(0, Bucket(disk)); // placeholder
	// TODO: implement partition phase
	for (uint32_t pId = left_rel.first; pId < left_rel.second; ++pId)
	{
		Page *pg = mem->mem_page(pId);
		mem->loadFromDisk(disk, pId, pId);

		for (uint32_t recId = 0; recId < pg->size(); ++recId)
		{
			Record r = pg->get_record(recId);
			uint32_t hash = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			partitions[hash].add_left_rel_page(pId);
		}
	}
	// what memory page id do we flush?
	// mem->flushToDisk(disk, )

	for (uint32_t pId = right_rel.first; pId < right_rel.second; ++pId)
	{
		Page *pg = mem->mem_page(pId);
		mem->loadFromDisk(disk, pId, pId);

		for (uint32_t recId = 0; recId < pg->size(); ++recId)
		{
			Record r = pg->get_record(recId);
			uint32_t hash = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
			partitions[hash].add_right_rel_page(pId);
		}
	}

	// what memory page id do we flush?
	// mem->flushToDisk(disk, )

	

	
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder
	for (uint32_t k = 0; k < MEM_SIZE_IN_PAGE; ++k)
	{
		Bucket& partition = partitions[k];
	}
	
	return disk_pages;
}
