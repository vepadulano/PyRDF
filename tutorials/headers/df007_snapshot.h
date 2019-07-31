#ifndef PYRDF_TUTORIALS_HEADERS_DF007_SNAPSHOT_H
#define PYRDF_TUTORIALS_HEADERS_DF007_SNAPSHOT_H

std::vector<float> getVector (float b2)
{
   std::vector<float> v;
   for (int i = 0; i < 3; i++) v.push_back(b2*i);
   return v;
}

#endif // PYRDF_TUTORIALS_HEADERS_DF007_SNAPSHOT_H
