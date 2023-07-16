import scipy.stats as st
from typing import Dict
import pandas as pd
import numpy as np
import logging
import os

async def on_start(
    logger: logging,
    path: os.PathLike,
    **kwargs
):
    logger.warn(f"Creating distribution")
    data_df: pd.DataFrame = pd.read_csv(path)
    pod_metric_group_df: pd.DataFrame = data_df.groupby(['pod', 'metric_name'])
    grouped_values: pd.DataFrame = pod_metric_group_df.apply(lambda x: x['value'])
    # for x,y in 
    # grouped_values: pd.DataFrame = data_df.apply(lambda x: x['value'])
    print(grouped_values.head())
    mean, std = st.norm.fit(grouped_values)
    distribution = st.gass(loc=mean, scale=std)

    # distribution = await _create_distribution_from_file(logger=logger,path=path)


async def on_end(
    logger: logging,
    path: os.PathLike,
    **kwargs
):
    # stats.distributions.ga
    pass

async def _create_distribution_from_file(
    logger: logging,
    path: os.PathLike,
) -> Dict[str, str]:
    data_df: pd.DataFrame = pd.read_csv(path)
    pod_metric_group_df: pd.DataFrame = data_df.groupby(['pod', 'metric_name'])
    pod_metric_distribution: pd.Series = pod_metric_group_df['value'].values
    return  _data_distribution(pod_metric_distribution)
    
    # return {
    #     "":  _data_distribution(pod_metric_group_df['value'])
    #     for row in pod_metric_group_df.index:
            
    # }
    """
    import scipy.stats as st
def get_best_distribution(data):
    dist_names = ["norm", "exponweib", "weibull_max", "weibull_min", "pareto", "genextreme"]
    dist_results = []
    params = {}
    for dist_name in dist_names:
        dist = getattr(st, dist_name)
        param = dist.fit(data)

        params[dist_name] = param
        # Applying the Kolmogorov-Smirnov test
        D, p = st.kstest(data, dist_name, args=param)
        print("p value for "+dist_name+" = "+str(p))
        dist_results.append((dist_name, p))

    # select the best fitted distribution
    best_dist, best_p = (max(dist_results, key=lambda item: item[1]))
    # store the name of the best fit and its p value

    print("Best fitting distribution: "+str(best_dist))
    print("Best p value: "+ str(best_p))
    print("Parameters for the best fit: "+ str(params[best_dist]))

    return best_dist, best_p, params[best_dist]
    """


def _data_distribution(samples: pd.Series) -> st.gaussian_kde:
    # TODO: peek best distribution to return to the client
    print("FKDMSFDS")
    print("$"*59)
    print(samples.array)
    print("$"*59)
    print("FKDMSFDS")
    return st.gaussian_kde(samples)


### 


