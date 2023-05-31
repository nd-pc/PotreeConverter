cmake_minimum_required(VERSION 3.16)
set(CMAKE_SUPPRESS_REGENERATION true)

project(PotreeConverterMPI LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)



set(target PotreeConverterMPI)

#message(${PROJECT_SOURCE_DIR})

set(HEADER_FILES  
	./Converter/include/Attributes.h
	./Converter/include/chunker_countsort_laszip.h
	./Converter/include/ChunkRefiner.h
	./Converter/include/ConcurrentWriter.h
	./Converter/include/converter_utils.h
	./Converter/include/indexer.h
	./Converter/include/prototyping.h
	./Converter/include/sampler_poisson.h
	./Converter/include/sampler_poisson_average.h
	./Converter/include/sampler_random.h
	./Converter/include/structures.h
	./Converter/include/Vector3.h
	./Converter/include/PotreeConverter.h
	./Converter/include/logger.h
	./Converter/modules/LasLoader/LasLoader.h
	./Converter/modules/unsuck/unsuck.hpp
		./Converter/include/record_timings.hpp
		./Converter/include/MPIcommon.h)

add_executable(${target}
	./Converter/src/chunker_countsort_laszip.cpp
	./Converter/src/indexer.cpp 
	./Converter/src/main.cpp
	./Converter/src/logger.cpp
	./Converter/modules/LasLoader/LasLoader.cpp
	./Converter/modules/unsuck/unsuck_platform_specific.cpp
	${HEADER_FILES}
)

set(LASZIP_DIR "${PROJECT_SOURCE_DIR}/Converter/libs/laszip")
add_subdirectory(${LASZIP_DIR})
target_link_libraries(${target} laszip)

set(BROTLI_DIR "${PROJECT_SOURCE_DIR}/Converter/libs/brotli")
add_subdirectory(${BROTLI_DIR})
target_link_libraries(${target} brotlienc-static)
target_link_libraries(${target} brotlidec-static)


target_include_directories(${target} PRIVATE "./Converter/include")
target_include_directories(${target} PRIVATE "./Converter/modules")
target_include_directories(${target} PRIVATE "./Converter/libs")




if (UNIX)
	find_package(Threads REQUIRED)
	find_package(TBB REQUIRED)

	find_package(MPI REQUIRED)
	
	target_link_libraries(${PROJECT_NAME} Threads::Threads)
	target_link_libraries(${PROJECT_NAME} tbb)
	target_include_directories(${target} PRIVATE ${MPI_CXX_INCLUDE_PATH})
	target_link_libraries(${target} ${MPI_CXX_LIBRARIES})
	target_link_libraries(${target} ${MPI_CXX_LINK_FLAGS})
	target_compile_options(${target} PRIVATE ${MPI_CXX_COMPILE_FLAGS})
	target_compile_definitions(${target} PRIVATE OMPI_SKIP_MPICXX)
	target_compile_options(${target} PRIVATE -g)

else()
	message(FATAL_ERROR "This is PotreeConverter MPI implementation. Currently it only supports UNIX clusters")

	#SET(CMAKE_CXX_FLAGS "-pthread -ltbb")
endif (UNIX)

###############################################
# COPY PAGE TEMPLATE TO BINARY DIRECTORY
###############################################

add_custom_command(
	TARGET ${target} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy_directory
		${PROJECT_SOURCE_DIR}/resources/page_template
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/resources/page_template)

###############################################
# COPY LICENSE FILES TO BINARY DIRECTORY
###############################################
add_custom_command(
	TARGET ${target} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy
		${LASZIP_DIR}/COPYING
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/licenses/license_laszip.txt)

add_custom_command(
	TARGET ${target} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy
		${BROTLI_DIR}/LICENSE
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/licenses/license_brotli.txt)

add_custom_command(
	TARGET ${target} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy
		${PROJECT_SOURCE_DIR}/Converter/libs/json/LICENSE
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/licenses/license_json.txt)

add_custom_command(
	TARGET ${target} POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy
		${PROJECT_SOURCE_DIR}/LICENSE
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/licenses/license_potree_converter.txt)

add_custom_command(
	TARGET PotreeConverterMPI POST_BUILD
	COMMAND ${CMAKE_COMMAND} -E copy
		${PROJECT_SOURCE_DIR}/README.md
		$<TARGET_FILE_DIR:${PROJECT_NAME}>/README.md)

