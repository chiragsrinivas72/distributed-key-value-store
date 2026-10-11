#ifndef HELPER_H
#define HELPER_H

#include <random>

namespace rafty{
    int randomBetween(uint64_t min, u_int64_t max);
    struct ElectionTracker {
        uint64_t term;           
        uint64_t votesReceived = 1;  
        uint64_t totalNodes;         

        bool hasMajority() {
            return votesReceived > (totalNodes / 2);
        }
    };
}


#endif //HELPER_H

