class ModelWrapper:
    """
    One complete model pipeline consisting of: 
    - DataLoader
    - Featurizer
    - MLForecast model(s)
    - PostProcessor

    Args:
        loader: DataLoader for loading model data.
        featurizer: Featurizer for joining feature tables and applying custom feature functions.
        fcst: MLForecast instance.
        postprocessor: PostProcessor for applying custom post-processing functions to model predictions.
    """

    def __init__(
        self,
        loader: DataLoader,
        featurizer: Featurizer,
        fcst: MLForecast,
        postprocessor: PostProcessor,
    ):
        self.loader = loader
        self.featurizer = featurizer
        self.fcst = fcst
        self.postprocessor = postprocessor