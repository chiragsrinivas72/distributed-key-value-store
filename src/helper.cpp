#include "rafty/helper.hpp"

namespace rafty{
    int randomBetween(uint64_t min, uint64_t max) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(min, max);

        return dist(gen);
    }
}
