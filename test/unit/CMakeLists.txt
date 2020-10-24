file(GLOB SRCS *.cpp)

ADD_EXECUTABLE(unittest ${SRCS})

TARGET_LINK_LIBRARIES(unittest
    libgtest
    libgmock
)

add_test(NAME unittest
         COMMAND unittest)
