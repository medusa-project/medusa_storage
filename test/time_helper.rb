module TimeHelper

  module_function

  def time_equal(time_1, time_2)
    time_1.to_f == time_2.to_f
  end

  def time_less_than_or_eq(time_1, time_2)
    time_1.to_f <= time_2.to_f
  end

end